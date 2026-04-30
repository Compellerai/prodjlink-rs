//! Pro DJ Link Network Source
//!
//! Native Rust implementation of Pioneer Pro DJ Link protocol for CDJ status monitoring.
//! Listens on UDP port 50002 for CDJ status packets and queries track metadata via TCP.
//!
//! Protocol documentation: https://djl-analysis.deepsymmetry.org/

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// CDJ device information for display or integration layers.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CdjDeviceInfo {
    pub device_id: u8,
    pub name: String,
    pub ip: IpAddr,
    pub track_loaded: bool,
    pub is_playing: bool,
    pub is_looping: bool,
    pub is_master: bool,
    pub is_on_air: bool,
    pub bpm: Option<f32>,
    pub track_title: Option<String>,
    pub track_artist: Option<String>,
}

/// Current track information resolved from a Pro DJ Link master deck.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TrackInfo {
    pub title: String,
    pub artist: String,
    pub album: Option<String>,
    pub duration: Option<f64>,
    pub position: Option<f64>,
    pub is_playing: bool,
    pub bpm: Option<f32>,
    pub source: String,
    pub deck: Option<u8>,
    pub artwork: Option<Vec<u8>>,
}


/// Pro DJ Link packet header magic bytes
const PRODJLINK_HEADER: [u8; 10] = [0x51, 0x73, 0x70, 0x74, 0x31, 0x57, 0x6d, 0x4a, 0x4f, 0x4c];

/// CDJ status packet type (port 50002)
const PACKET_TYPE_CDJ_STATUS: u8 = 0x0a;

/// Database query magic bytes
const DB_MAGIC: [u8; 4] = [0x87, 0x23, 0x49, 0xae];

/// Pro DJ Link network source - discovers and monitors Pioneer/AlphaTheta CDJs
pub struct ProDjLinkClient {
    state: Arc<Mutex<ProDjLinkState>>,
    /// Separate lock for track history to avoid blocking render thread
    track_history: Arc<Mutex<Vec<TrackChangeRecord>>>,
    listener_thread: Option<JoinHandle<()>>,
    running: Arc<AtomicBool>,
}

/// Pending UDP packet with source info for filtering
struct PendingUdpPacket {
    data: Vec<u8>,
    source_ip: IpAddr,
    device_id: u8,
}

struct ProDjLinkState {
    /// Connected CDJ devices by device ID (1-6)
    devices: HashMap<u8, CDJDevice>,
    /// Track metadata cache: (device_id, slot, track_id) -> metadata
    metadata_cache: HashMap<MetadataKey, TrackMetadata>,
    /// Pending metadata fetches to avoid duplicate requests
    pending_fetches: std::collections::HashSet<MetadataKey>,
    /// Last error message for diagnostics
    last_error: Option<String>,
    /// Pending metadata UDP packets (type=0x40) with source IP for filtering
    pending_metadata_packets: Vec<PendingUdpPacket>,
    /// Our virtual device ID (dynamically selected to avoid conflicts)
    our_device_id: u8,
    /// Last track change time per device for debouncing fetches
    last_track_change: HashMap<u8, Instant>,
}

/// Record of a track change on a deck (for database registration)
#[derive(Clone, Debug)]
pub struct TrackChangeRecord {
    /// When the track change was detected
    pub timestamp: Instant,
    /// Deck number (1-6)
    pub deck: u8,
    /// Track ID in rekordbox database
    pub track_id: u32,
    /// Source device ID (which device has the USB/SD)
    pub source_device: u8,
    /// Slot (1=CD, 2=SD, 3=USB)
    pub slot: u8,
    /// Track title (if metadata available)
    pub title: Option<String>,
    /// Track artist (if metadata available)
    pub artist: Option<String>,
    /// BPM (if available)
    pub bpm: Option<f32>,
    /// Whether deck is master
    pub is_master: bool,
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct MetadataKey {
    device_id: u8,
    slot: u8,
    track_id: u32,
}

#[derive(Clone, Debug)]
struct TrackMetadata {
    title: String,
    artist: String,
    #[allow(dead_code)]
    album: Option<String>,
    duration: Option<f64>,
    #[allow(dead_code)]
    fetched_at: Instant,
}

#[derive(Debug)]
struct CDJDevice {
    device_id: u8,
    name: String,
    ip: IpAddr,
    last_seen: Instant,
    status: DeckStatus,
}

#[derive(Default, Clone, Debug)]
struct DeckStatus {
    track_loaded: bool,
    is_playing: bool,
    is_looping: bool,
    is_master: bool,
    is_on_air: bool,
    bpm: Option<f32>,
    track_device_id: u8,
    track_slot: u8,
    track_id: u32,
    #[allow(dead_code)]
    beat_in_measure: u8,
}

impl ProDjLinkClient {
    /// Create a new Pro DJ Link source and start listening for CDJs
    pub fn new() -> Self {
        let state = Arc::new(Mutex::new(ProDjLinkState {
            devices: HashMap::new(),
            metadata_cache: HashMap::new(),
            pending_fetches: std::collections::HashSet::new(),
            last_error: None,
            pending_metadata_packets: Vec::new(),
            our_device_id: 6, // Will be updated after network scan
            last_track_change: HashMap::new(),
        }));
        let track_history = Arc::new(Mutex::new(Vec::new()));
        let running = Arc::new(AtomicBool::new(true));

        let thread_state = state.clone();
        let thread_track_history = track_history.clone();
        let thread_running = running.clone();

        let listener_thread = thread::Builder::new()
            .name("prodjlink-listener".to_string())
            .spawn(move || {
                if let Err(e) = Self::listener_loop(thread_state, thread_track_history, thread_running) {
                    log::error!("[ProDjLink] Listener error: {}", e);
                }
            })
            .expect("Failed to spawn Pro DJ Link listener thread");

        log::info!("[ProDjLink] Starting Pro DJ Link listener...");

        Self {
            state,
            track_history,
            listener_thread: Some(listener_thread),
            running,
        }
    }

    /// Get the local IP address for the interface that can reach the broadcast network
    fn get_local_ip() -> Option<[u8; 4]> {
        // Try to find a suitable local IP by connecting to a broadcast-capable route
        let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
        // Connect to a local broadcast address to determine our interface
        socket.connect("10.255.255.255:1").ok()?;
        let local_addr = socket.local_addr().ok()?;
        if let IpAddr::V4(ip) = local_addr.ip() {
            Some(ip.octets())
        } else {
            None
        }
    }

    /// Get a pseudo-MAC address (use local IP + fixed bytes for uniqueness)
    fn get_pseudo_mac(local_ip: &[u8; 4]) -> [u8; 6] {
        // Create a unique-ish MAC: 02:xx:xx:IP:IP:IP (02 = locally administered)
        [0x02, 0xC0, 0x11, local_ip[1], local_ip[2], local_ip[3]]
    }

    /// Build a CDJ status packet (type 0x0a) to broadcast our state on port 50002
    /// CDJs only send status to devices that are also broadcasting status
    fn build_status_packet(device_id: u8, device_name: &str, packet_counter: u8) -> Vec<u8> {
        let mut packet = Vec::with_capacity(212); // 0xD4 bytes

        // Header (bytes 0x00-0x09): Pro DJ Link magic
        packet.extend_from_slice(&PRODJLINK_HEADER);

        // Packet type (byte 0x0a): 0x0a = CDJ status
        packet.push(0x0a);

        // Device name (bytes 0x0b-0x1f): 21 bytes, null-padded
        let name_bytes = device_name.as_bytes();
        let name_len = name_bytes.len().min(20);
        packet.extend_from_slice(&name_bytes[..name_len]);
        packet.extend(std::iter::repeat(0u8).take(21 - name_len));

        // Byte 0x20: always 0x01
        packet.push(0x01);

        // Byte 0x21: device number
        packet.push(device_id);

        // Bytes 0x22-0x23: packet length (0x00d4 = 212)
        packet.extend_from_slice(&[0x00, 0xd4]);

        // Bytes 0x24-0x27: zeros
        packet.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

        // Byte 0x28: track source device (0 = no track)
        packet.push(0x00);

        // Byte 0x29: track slot (0 = none)
        packet.push(0x00);

        // Bytes 0x2a-0x2b: zeros
        packet.extend_from_slice(&[0x00, 0x00]);

        // Bytes 0x2c-0x2f: track ID (0 = none)
        packet.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

        // Fill bytes 0x30-0x7a with zeros (general status area)
        packet.extend(std::iter::repeat(0u8).take(0x7b - 0x30));

        // Byte 0x7b: play state (0x00 = no track)
        packet.push(0x00);

        // Fill bytes 0x7c-0x88 with zeros
        packet.extend(std::iter::repeat(0u8).take(0x89 - 0x7c));

        // Byte 0x89: flags (0x00 = not master, not on-air, not synced)
        packet.push(0x00);

        // Fill bytes 0x8a-0x91 with zeros
        packet.extend(std::iter::repeat(0u8).take(0x92 - 0x8a));

        // Bytes 0x92-0x93: BPM x 100 (0 = none)
        packet.extend_from_slice(&[0x00, 0x00]);

        // Fill remaining bytes up to 0xc8 with zeros
        packet.extend(std::iter::repeat(0u8).take(0xc8 - 0x94));

        // Byte 0xc8: packet counter
        packet.push(packet_counter);

        // Fill remaining bytes to reach 212 total
        while packet.len() < 212 {
            packet.push(0x00);
        }

        packet
    }

    /// Build a keep-alive packet (type 0x06) to announce ourselves on the network
    /// Per djl-analysis.deepsymmetry.org: https://djl-analysis.deepsymmetry.org/djl-analysis/startup.html#cdj-keep-alive
    fn build_keep_alive_packet(device_id: u8, device_name: &str, mac: &[u8; 6], ip: &[u8; 4], peer_count: u8) -> Vec<u8> {
        let mut packet = Vec::with_capacity(54);

        // 0x00-0x09: Pro DJ Link magic header
        packet.extend_from_slice(&PRODJLINK_HEADER);

        // 0x0a: Packet type = 0x06 (keep-alive)
        packet.push(0x06);

        // 0x0b-0x0d: Fixed bytes (NOT device name!)
        packet.extend_from_slice(&[0x00, 0x00, 0x10]);

        // 0x0e-0x1f: Device name (18 bytes, null-padded)
        let name_bytes = device_name.as_bytes();
        let name_len = name_bytes.len().min(18);
        packet.extend_from_slice(&name_bytes[..name_len]);
        packet.extend(std::iter::repeat(0u8).take(18 - name_len));

        // 0x20: Fixed = 0x01
        packet.push(0x01);

        // 0x21: Structure type = 0x02
        packet.push(0x02);

        // 0x22-0x23: Packet length = 0x0036 (54 bytes)
        packet.extend_from_slice(&[0x00, 0x36]);

        // 0x24: Device number
        packet.push(device_id);

        // 0x25: Device type = 0x01 (CDJ type)
        packet.push(0x01);

        // 0x26-0x2b: MAC address (6 bytes)
        packet.extend_from_slice(mac);

        // 0x2c-0x2f: IP address (4 bytes)
        packet.extend_from_slice(ip);

        // 0x30: Fixed = 0x30 (NOT 0x00!)
        packet.push(0x30);

        // 0x31: Peer count (number of devices we've seen)
        packet.push(peer_count);

        // 0x32-0x34: Fixed = 0x00 0x00 0x01 (NOT all zeros!)
        packet.extend_from_slice(&[0x00, 0x00, 0x01]);

        // 0x35: Fixed = 0x01
        packet.push(0x01);

        packet
    }

    /// Main listener loop - joins network and receives UDP packets
    fn listener_loop(
        state: Arc<Mutex<ProDjLinkState>>,
        track_history: Arc<Mutex<Vec<TrackChangeRecord>>>,
        running: Arc<AtomicBool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use socket2::{Domain, Protocol, Socket, Type};

        // Get our local IP address
        let local_ip = match Self::get_local_ip() {
            Some(ip) => ip,
            None => {
                let msg = "Could not determine local IP address".to_string();
                log::warn!("[ProDjLink] {}", msg);
                state.lock().last_error = Some(msg);
                return Ok(());
            }
        };
        let local_mac = Self::get_pseudo_mac(&local_ip);

        log::info!(
            "[ProDjLink] Local IP: {}.{}.{}.{}, MAC: {:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            local_ip[0], local_ip[1], local_ip[2], local_ip[3],
            local_mac[0], local_mac[1], local_mac[2], local_mac[3], local_mac[4], local_mac[5]
        );

        // Calculate broadcast address (assume /24 network)
        let broadcast_ip = [local_ip[0], local_ip[1], local_ip[2], 255];
        let broadcast_addr: SocketAddr = format!(
            "{}.{}.{}.{}:50000",
            broadcast_ip[0], broadcast_ip[1], broadcast_ip[2], broadcast_ip[3]
        ).parse()?;

        // Create socket for sending keep-alives (port 50000)
        let announce_socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        announce_socket.set_reuse_address(true)?;
        announce_socket.set_broadcast(true)?;
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = announce_socket.as_raw_fd();
            unsafe {
                let optval: libc::c_int = 1;
                libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t);
            }
        }
        let announce_bind: SocketAddr = "0.0.0.0:50000".parse()?;
        match announce_socket.bind(&announce_bind.into()) {
            Ok(_) => log::info!("[ProDjLink] Bound to UDP port 50000 (announcements)"),
            Err(e) => {
                log::warn!("[ProDjLink] Failed to bind port 50000: {} - will try listen-only mode", e);
            }
        }
        let announce_socket: UdpSocket = announce_socket.into();

        // Create socket to RECEIVE status packets on port 50002
        // CDJs will send status directly to us once they see our keep-alives
        let status_socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        status_socket.set_reuse_address(true)?;
        status_socket.set_nonblocking(true)?;
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = status_socket.as_raw_fd();
            unsafe {
                let optval: libc::c_int = 1;
                libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT,
                    &optval as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t);
            }
        }
        let status_bind: SocketAddr = "0.0.0.0:50002".parse()?;
        match status_socket.bind(&status_bind.into()) {
            Ok(_) => log::info!("[ProDjLink] Bound to UDP port 50002 (receiving CDJ status)"),
            Err(e) => {
                let msg = format!("Failed to bind port 50002: {} (rekordbox running?)", e);
                log::warn!("[ProDjLink] {}", msg);
                state.lock().last_error = Some(msg);
                return Ok(());
            }
        }
        let status_socket: UdpSocket = status_socket.into();

        let device_name = "REACT";

        // Broadcast address for status packets (port 50002)
        let status_broadcast_addr: SocketAddr = format!(
            "{}.{}.{}.{}:50002",
            broadcast_ip[0], broadcast_ip[1], broadcast_ip[2], broadcast_ip[3]
        ).parse()?;

        let mut buf = [0u8; 512];

        // Store our local IP to filter out our own packets
        let local_ip_addr: IpAddr = format!("{}.{}.{}.{}", local_ip[0], local_ip[1], local_ip[2], local_ip[3])
            .parse()
            .unwrap();

        // Phase 1: Scan network for 2 seconds to find used device IDs
        log::info!("[ProDjLink] Scanning network for existing CDJs...");
        let mut used_ids: std::collections::HashSet<u8> = std::collections::HashSet::new();
        let scan_start = Instant::now();
        while scan_start.elapsed() < Duration::from_secs(2) && running.load(Ordering::Relaxed) {
            // Check port 50000 for keep-alive packets
            announce_socket.set_nonblocking(true).ok();
            if let Ok((len, _src)) = announce_socket.recv_from(&mut buf) {
                if len >= 0x36 && buf[..10] == PRODJLINK_HEADER && buf[0x0a] == 0x06 {
                    let peer_id = buf[0x24];
                    if (1..=6).contains(&peer_id) {
                        used_ids.insert(peer_id);
                    }
                }
            }
            // Also check port 50002 for status packets
            if let Ok((len, _src)) = status_socket.recv_from(&mut buf) {
                if len >= 0x22 && buf[..10] == PRODJLINK_HEADER && buf[0x0a] == 0x0a {
                    let peer_id = buf[0x21];
                    if (1..=6).contains(&peer_id) {
                        used_ids.insert(peer_id);
                    }
                }
            }
            thread::sleep(Duration::from_millis(50));
        }

        // Pick an unused ID, preferring 6 first then working down to 1
        // If all 1-6 are used, use 7 as fallback (CDJ-3000s might accept it)
        let device_id: u8 = [6, 5, 4, 3, 2, 1]
            .into_iter()
            .find(|id| !used_ids.contains(id))
            .unwrap_or(7);

        if !used_ids.is_empty() {
            log::info!("[ProDjLink] Found CDJs using IDs: {:?}, selecting ID {}", used_ids, device_id);
        } else {
            log::info!("[ProDjLink] No CDJs found during scan, using ID {}", device_id);
        }

        // Store our device ID in state for metadata fetch to use
        {
            let mut s = state.lock();
            s.our_device_id = device_id;
        }

        let mut last_announce = Instant::now() - Duration::from_secs(10); // Trigger immediate announce
        let mut last_status = Instant::now() - Duration::from_secs(10); // Trigger immediate status
        let mut last_prune = Instant::now();
        let mut packet_counter: u8 = 0;

        log::info!("[ProDjLink] Joining Pro DJ Link network as '{}' (ID {})...", device_name, device_id);

        while running.load(Ordering::Relaxed) {
            // Send keep-alive announcement every 1.5 seconds on port 50000
            if last_announce.elapsed() >= Duration::from_millis(1500) {
                // Get peer count (number of CDJs we've discovered)
                let peer_count = state.lock().devices.len() as u8;
                let packet = Self::build_keep_alive_packet(device_id, device_name, &local_mac, &local_ip, peer_count);
                match announce_socket.send_to(&packet, broadcast_addr) {
                    Ok(_) => {
                        log::trace!("[ProDjLink] Sent keep-alive to {} (peers: {})", broadcast_addr, peer_count);
                    }
                    Err(e) => {
                        log::debug!("[ProDjLink] Failed to send keep-alive: {}", e);
                    }
                }
                last_announce = Instant::now();
            }

            // Send status packet every 200ms on port 50002 to appear as a real player
            // This is required for metadata queries - CDJs only respond to "real" players
            if last_status.elapsed() >= Duration::from_millis(200) {
                let status_packet = Self::build_status_packet(device_id, device_name, packet_counter);
                packet_counter = packet_counter.wrapping_add(1);

                // Broadcast status to all devices on the network
                match status_socket.send_to(&status_packet, status_broadcast_addr) {
                    Ok(_) => {
                        log::trace!("[ProDjLink] Sent status packet (counter: {})", packet_counter);
                    }
                    Err(e) => {
                        log::debug!("[ProDjLink] Failed to send status: {}", e);
                    }
                }
                last_status = Instant::now();
            }

            // Read status packets from port 50002
            // CDJs send status directly to us after seeing our keep-alives
            match status_socket.recv_from(&mut buf) {
                Ok((len, src)) => {
                    // Skip our own packets (we receive our own broadcasts)
                    if src.ip() == local_ip_addr {
                        continue;
                    }

                    // Validate Pro DJ Link header
                    if len >= 11 && buf[..10] == PRODJLINK_HEADER {
                        let packet_type = buf[0x0a];

                        if packet_type == PACKET_TYPE_CDJ_STATUS {
                            // Full CDJ status packet (type 0x0a) - most common, don't log
                            Self::parse_cdj_status(&buf[..len], src.ip(), &state, &track_history);
                        } else if packet_type == 0x40 {
                            // Type 0x40: mixer on-air status - store with source info for filtering
                            let source_device_id = if len >= 0x22 { buf[0x21] } else { 0 };
                            {
                                let mut s = state.lock();
                                if s.pending_metadata_packets.len() > 50 {
                                    s.pending_metadata_packets.remove(0);
                                }
                                s.pending_metadata_packets.push(PendingUdpPacket {
                                    data: buf[..len].to_vec(),
                                    source_ip: src.ip(),
                                    device_id: source_device_id,
                                });
                            }
                            Self::parse_mixer_on_air(&buf[..len], &state);
                        }
                        // Silently ignore other packet types
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(e) => {
                    log::info!("[ProDjLink] Port 50002 recv error: {}", e);
                }
            }

            // Also check port 50000 for other device announcements (to discover CDJs)
            announce_socket.set_nonblocking(true).ok();
            if let Ok((len, src)) = announce_socket.recv_from(&mut buf) {
                if len >= 11 && buf[..10] == PRODJLINK_HEADER {
                    let packet_type = buf[0x0a];
                    // Type 0x06 = keep-alive from other devices
                    if packet_type == 0x06 && len >= 0x36 {
                        // Device number is at byte 0x24 (36)
                        let peer_id = buf[0x24];

                        // Device name: bytes 0x0c to 0x1f (12-31), 20 bytes max
                        let name_bytes = &buf[0x0c..0x20.min(len)];
                        let name_end = name_bytes.iter().position(|&b| b == 0).unwrap_or(name_bytes.len());
                        let name = String::from_utf8_lossy(&name_bytes[..name_end]).trim().to_string();

                        if peer_id != device_id && (1..=6).contains(&peer_id) {
                            let mut st = state.lock();
                            let is_new = !st.devices.contains_key(&peer_id);

                            // Update or insert device (will get full status from port 50002)
                            st.devices.entry(peer_id).or_insert_with(|| CDJDevice {
                                device_id: peer_id,
                                name: name.clone(),
                                ip: src.ip(),
                                last_seen: Instant::now(),
                                status: DeckStatus::default(),
                            }).last_seen = Instant::now();

                            // Also update name if we got a better one
                            if !name.is_empty() {
                                if let Some(dev) = st.devices.get_mut(&peer_id) {
                                    if dev.name.is_empty() {
                                        dev.name = name.clone();
                                    }
                                }
                            }

                            if is_new {
                                log::info!(
                                    "[ProDjLink] Discovered CDJ: {} (ID {}) at {}",
                                    name, peer_id, src.ip()
                                );
                            }
                        }
                    }
                }
            }

            // Prune stale devices every 2 seconds
            if last_prune.elapsed() > Duration::from_secs(2) {
                let mut state = state.lock();
                let now = Instant::now();
                let before = state.devices.len();
                state
                    .devices
                    .retain(|_, d| now.duration_since(d.last_seen) < Duration::from_secs(10));
                let after = state.devices.len();
                if before != after {
                    log::info!(
                        "[ProDjLink] Pruned {} stale device(s), {} remaining",
                        before - after,
                        after
                    );
                }
                last_prune = Instant::now();
            }
        }

        log::info!("[ProDjLink] Listener stopped");
        Ok(())
    }

    /// Parse mixer on-air status packet (type 0x40)
    /// This packet tells us which mixer channels have faders up (on-air)
    fn parse_mixer_on_air(data: &[u8], state: &Arc<Mutex<ProDjLinkState>>) {
        // Mixer on-air packets are typically 0x26 (38) bytes
        // The on-air status for channels 1-4 is at byte 0x27 as a bitmask
        // Bit 0 = channel 1, Bit 1 = channel 2, etc.
        if data.len() < 0x28 {
            log::trace!("[ProDjLink] Mixer packet too short: {} bytes", data.len());
            return;
        }

        // Extract channel on-air bitmask
        // Different mixers may have slightly different formats
        // DJM-900NXS2/A9: byte 0x27 contains channel on-air flags
        let on_air_mask = data[0x27];
        log::trace!("[ProDjLink] Mixer on-air mask: 0x{:02x}", on_air_mask);

        // Update on-air status for each device that matches a channel
        let mut st = state.lock();
        for device in st.devices.values_mut() {
            // Mixer channels 1-4 correspond to CDJ device IDs 1-4
            if (1..=4).contains(&device.device_id) {
                let channel_bit = 1u8 << (device.device_id - 1);
                let new_on_air = (on_air_mask & channel_bit) != 0;
                if device.status.is_on_air != new_on_air {
                    log::info!("[ProDjLink] Deck {} on-air: {} -> {} (mixer)", device.device_id, device.status.is_on_air, new_on_air);
                    device.status.is_on_air = new_on_air;
                }
            }
        }
    }

    /// Parse a CDJ status packet (type 0x0a on port 50002)
    fn parse_cdj_status(
        data: &[u8],
        src_ip: IpAddr,
        state: &Arc<Mutex<ProDjLinkState>>,
        track_history: &Arc<Mutex<Vec<TrackChangeRecord>>>,
    ) {
        // CDJ status packets vary in length - minimum ~0x7C needed for basic info
        if data.len() < 0x7C {
            return;
        }

        // Device number at offset 0x21
        let device_id = data[0x21];
        // Filter: valid CDJ range (1-6), but exclude our own virtual device
        let our_id = state.lock().our_device_id;
        if !(1..=6).contains(&device_id) || device_id == our_id {
            return;
        }

        // Extract device name (bytes 0x0b-0x1f, null-terminated ASCII)
        let name_end = data[0x0b..0x20]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(0x15);
        let name = String::from_utf8_lossy(&data[0x0b..0x0b + name_end])
            .trim()
            .to_string();

        // P1 (byte 0x7B) - playback mode:
        // 0x00 = No track, 0x02 = Stopped, 0x03 = Playing, 0x04 = Playing loop,
        // 0x05 = Paused, 0x06 = Cued, 0x09 = Searching, 0x11 = Loading, 0x12 = Emergency loop
        let play_mode = data[0x7B];
        let is_looping = play_mode == 0x04 || play_mode == 0x12; // Playing loop or emergency loop

        // F (byte 0x89) - flags:
        // Bit 6 (0x40) = Playing (1=playing, 0=idle)
        // Bit 5 (0x20) = Is Master
        // Bit 3 (0x08) = On-air
        let flags = data[0x89];
        let is_playing = (flags & 0x40) != 0;
        let is_master = (flags & 0x20) != 0;
        let is_on_air = (flags & 0x08) != 0;

        // Track source info
        let track_device_id = data[0x28];
        let track_slot = data[0x29]; // 1=CD, 2=SD, 3=USB, 4=rekordbox
        let track_id = u32::from_be_bytes([data[0x2C], data[0x2D], data[0x2E], data[0x2F]]);
        let track_loaded = track_id != 0;

        // BPM at offset 0x92-0x93 (value x 100) - check bounds first
        let bpm = if data.len() > 0x93 {
            let bpm_raw = u16::from_be_bytes([data[0x92], data[0x93]]);
            if bpm_raw > 0 && bpm_raw < 30000 {
                Some(bpm_raw as f32 / 100.0)
            } else {
                None
            }
        } else {
            None
        };

        // Beat in measure at offset 0xA6 (1-4) - check bounds
        let beat_in_measure = if data.len() > 0xA6 { data[0xA6] } else { 0 };

        let new_status = DeckStatus {
            track_loaded,
            is_playing,
            is_looping,
            is_master,
            is_on_air,
            bpm,
            track_device_id,
            track_slot,
            track_id,
            beat_in_measure,
        };

        // Quick lock to check state and update - minimize lock duration
        let (is_new, track_changed, cached_title, cached_artist) = {
            let mut state = state.lock();
            let is_new = !state.devices.contains_key(&device_id);

            // Check if track changed
            let track_changed = if let Some(existing) = state.devices.get(&device_id) {
                existing.status.track_id != track_id && track_id != 0
            } else {
                track_id != 0
            };

            // Record track change time for debouncing metadata fetches
            if track_changed {
                state.last_track_change.insert(device_id, Instant::now());
            }

            // Look up cached metadata if track changed
            let (cached_title, cached_artist) = if track_changed {
                let metadata_key = MetadataKey {
                    device_id: track_device_id,
                    slot: track_slot,
                    track_id,
                };
                state.metadata_cache.get(&metadata_key)
                    .map(|m| (Some(m.title.clone()), Some(m.artist.clone())))
                    .unwrap_or((None, None))
            } else {
                (None, None)
            };

            // Update device state
            state.devices.insert(
                device_id,
                CDJDevice {
                    device_id,
                    name: name.clone(),
                    ip: src_ip,
                    last_seen: Instant::now(),
                    status: new_status,
                },
            );

            (is_new, track_changed, cached_title, cached_artist)
        }; // state lock released here

        // Record track change AFTER releasing state lock (separate lock)
        if track_changed && track_id != 0 {
            let record = TrackChangeRecord {
                timestamp: Instant::now(),
                deck: device_id,
                track_id,
                source_device: track_device_id,
                slot: track_slot,
                title: cached_title,
                artist: cached_artist,
                bpm,
                is_master,
            };

            // Use separate lock for track history - doesn't block state access
            let mut history = track_history.lock();
            if history.len() >= 100 {
                history.remove(0);
            }
            history.push(record);
        }

        // Log new device discovery (outside of any lock)
        if is_new {
            log::info!(
                "[ProDjLink] CDJ detected: {} (ID {}) at {} - playing={}, master={}, track={}, bpm={:?}",
                name, device_id, src_ip, is_playing, is_master, track_id, bpm
            );
        }
    }

    /// Discover the dbserver port by querying port 12523
    fn discover_dbserver_port(device_ip: IpAddr) -> Option<u16> {
        let addr: SocketAddr = format!("{}:12523", device_ip).parse().ok()?;
        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;

        // Send discovery request: magic + "RemoteDBServer"
        // Format: 00 00 00 0f "RemoteDBServer" 00
        let request = b"\x00\x00\x00\x0fRemoteDBServer\x00";
        stream.write_all(request).ok()?;

        // Response is 2 bytes: port number in big-endian
        let mut resp = [0u8; 2];
        stream.read_exact(&mut resp).ok()?;
        let port = u16::from_be_bytes(resp);

        log::info!("[ProDjLink] Discovered dbserver port: {}", port);
        Some(port)
    }

    /// Fetch track metadata from CDJ database via TCP (dbserver protocol)
    /// Protocol: https://djl-analysis.deepsymmetry.org/djl-analysis/track_metadata.html
    fn fetch_metadata(
        device_ip: IpAddr,
        track_device_id: u8,
        track_slot: u8,
        track_id: u32,
        state: &Arc<Mutex<ProDjLinkState>>,
    ) -> Option<TrackMetadata> {
        log::debug!("[ProDjLink] Fetching metadata: device={}, slot={}, track={} from {}",
            track_device_id, track_slot, track_id, device_ip);

        // First try to discover the dbserver port (CDJ-3000 might use different port)
        let db_port = Self::discover_dbserver_port(device_ip).unwrap_or(1051);

        // Connect to CDJ database port
        let addr: SocketAddr = format!("{}:{}", device_ip, db_port).parse().ok()?;
        let mut stream = match TcpStream::connect_timeout(&addr, Duration::from_secs(3)) {
            Ok(s) => {
                log::debug!("[ProDjLink] TCP connected to {}:{}", device_ip, db_port);
                s
            }
            Err(e) => {
                log::warn!("[ProDjLink] TCP connect failed {}:{} - {}", device_ip, db_port, e);
                return None;
            }
        };
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
        stream.set_write_timeout(Some(Duration::from_secs(5))).ok()?;
        stream.set_nodelay(true).ok()?;

        // Get our virtual device ID from state (dynamically selected during network scan)
        let our_device_id: u8 = state.lock().our_device_id;

        // Step 1: NumberField greeting (value=1, size=4)
        let greeting = [0x11, 0x00, 0x00, 0x00, 0x01];
        stream.write_all(&greeting).ok()?;
        stream.flush().ok()?;

        // Read response - should echo back same 5 bytes
        let mut resp = [0u8; 16];
        let n = match stream.read(&mut resp) {
            Ok(n) => n,
            Err(e) => {
                log::warn!("[ProDjLink] Greeting failed: {}", e);
                return None;
            }
        };

        if n == 0 {
            log::warn!("[ProDjLink] Greeting failed: CDJ closed connection");
            return None;
        }

        // Step 2: Context/setup request using dbserver protocol
        let setup_req = Self::build_setup_request(our_device_id, track_device_id);
        stream.write_all(&setup_req).ok()?;
        stream.flush().ok()?;

        let mut setup_resp = [0u8; 64];
        let n2 = match stream.read(&mut setup_resp) {
            Ok(n) => n,
            Err(e) => {
                log::warn!("[ProDjLink] Setup failed: {}", e);
                return None;
            }
        };

        if n2 == 0 {
            log::warn!("[ProDjLink] Setup failed: no response");
            return None;
        }

        // Parse Step 2 response - should be type 0x4000 (success)
        if n2 >= 13 {
            let setup_type = u16::from_be_bytes([setup_resp[11], setup_resp[12]]);
            if setup_type != 0x4000 {
                log::debug!("[ProDjLink] Setup response type=0x{:04x} (expected 0x4000)", setup_type);
            }
        }

        // Brief delay to let CDJ process setup
        std::thread::sleep(Duration::from_millis(150));

        // Step 3: Request track metadata (type 0x2002)
        let metadata_req = Self::build_metadata_request(our_device_id, track_device_id, track_slot, track_id);
        stream.write_all(&metadata_req).ok()?;

        let mut header = [0u8; 256];
        let n = match stream.read(&mut header) {
            Ok(n) => n,
            Err(e) => {
                log::warn!("[ProDjLink] Metadata request failed: {}", e);
                return None;
            }
        };

        if n < 15 {
            log::warn!("[ProDjLink] Metadata response too short: {} bytes", n);
            return None;
        }

        // Parse response: type, txid, argc
        let resp_txid = u32::from_be_bytes([header[6], header[7], header[8], header[9]]);
        let resp_type = u16::from_be_bytes([header[11], header[12]]);
        let resp_argc = header[14];

        // Extract menu_size from arg2
        let mut menu_size: u32 = 16;
        if resp_type == 0x4000 && n >= 32 && header[15] == 0x14 {
            let argtypes_len = u32::from_be_bytes([header[16], header[17], header[18], header[19]]) as usize;
            let args_start = 20 + argtypes_len;
            if n >= args_start + 10 && header[args_start] == 0x11 && header[args_start + 5] == 0x11 {
                menu_size = u32::from_be_bytes([
                    header[args_start + 6], header[args_start + 7],
                    header[args_start + 8], header[args_start + 9],
                ]);
            }
        }

        log::debug!("[ProDjLink] Metadata: type=0x{:04x}, txid={}, argc={}, menu_size={}",
            resp_type, resp_txid, resp_argc, menu_size);

        if resp_type == 0x4003 {
            log::debug!("[ProDjLink] Track metadata not available (0x4003)");
            return None;
        }

        if resp_type == 0x0100 {
            log::debug!("[ProDjLink] Error response 0x0100");
        }

        // Step 4: Render request (type 0x3000) to get actual metadata strings
        let render_txid = resp_txid.wrapping_add(1);
        let render_req = Self::build_render_request(our_device_id, track_device_id, track_slot, track_id, menu_size, menu_size, render_txid);
        stream.write_all(&render_req).ok()?;

        // Read initial TCP response
        let mut ack_buf = [0u8; 4096];
        let ack_n = stream.read(&mut ack_buf).unwrap_or(0);

        // Log framing info + first 32 bytes only
        if ack_n >= 13 {
            let ack_type = u16::from_be_bytes([ack_buf[11], ack_buf[12]]);
            log::debug!("[ProDjLink] Render ack: {} bytes, type=0x{:04x}, first32={:02x?}",
                ack_n, ack_type, &ack_buf[..ack_n.min(32)]);
        }

        // CDJ-3000 commonly streams render rows back on TCP after the ack
        if let Some(meta) = Self::parse_render_response(&mut stream) {
            log::debug!("[ProDjLink] Got metadata from TCP render stream");
            return Some(meta);
        }

        // Fallback: some setups may still emit UDP menu packets
        // Filter packets by expected source device to avoid "wrong player answered"
        std::thread::sleep(Duration::from_millis(300));
        let menu_packets: Vec<Vec<u8>> = {
            let mut s = state.lock();
            let packets: Vec<Vec<u8>> = s.pending_metadata_packets.iter()
                .filter(|p| p.device_id == track_device_id || p.source_ip == device_ip)
                .map(|p| p.data.clone())
                .collect();
            s.pending_metadata_packets.clear();
            packets
        };

        if !menu_packets.is_empty() {
            log::debug!("[ProDjLink] Trying {} filtered UDP packets from device {}", menu_packets.len(), track_device_id);
            Self::parse_menu_packets(&menu_packets)
        } else {
            None
        }
    }

    /// Parse menu item UDP packets for title/artist strings
    fn parse_menu_packets(packets: &[Vec<u8>]) -> Option<TrackMetadata> {
        let mut title = String::new();
        let mut artist = String::new();

        for packet in packets {
            // Scan for UTF-16BE strings (0x26 prefix)
            let mut i = 0;
            while i < packet.len().saturating_sub(6) {
                if packet[i] == 0x26 {
                    // UTF-16BE string: 0x26 + 4-byte length + UTF-16BE data
                    if i + 5 > packet.len() {
                        break;
                    }
                    let len = u32::from_be_bytes([
                        packet[i + 1],
                        packet[i + 2],
                        packet[i + 3],
                        packet[i + 4],
                    ]) as usize;

                    if len > 0 && len < 512 && i + 5 + len <= packet.len() {
                        let utf16_data = &packet[i + 5..i + 5 + len];

                        // Convert UTF-16BE to String
                        let mut utf16_vec = Vec::new();
                        for chunk in utf16_data.chunks(2) {
                            if chunk.len() == 2 {
                                utf16_vec.push(u16::from_be_bytes([chunk[0], chunk[1]]));
                            }
                        }

                        if let Ok(s) = String::from_utf16(&utf16_vec) {
                            let s = s.trim_matches('\0').trim().to_string();
                            if !s.is_empty() {
                                // First non-empty string is typically title, second is artist
                                if title.is_empty() {
                                    title = s;
                                } else if artist.is_empty() {
                                    artist = s;
                                }
                            }
                        }
                        i += 5 + len;
                        continue;
                    }
                }
                i += 1;
            }
        }

        if title.is_empty() {
            log::debug!("[ProDjLink] No title found in {} UDP packets", packets.len());
            return None;
        }

        log::debug!("[ProDjLink] Parsed from UDP: \"{}\" by \"{}\"", title, artist);

        Some(TrackMetadata {
            title,
            artist: if artist.is_empty() {
                "Unknown Artist".to_string()
            } else {
                artist
            },
            album: None,
            duration: None,
            fetched_at: Instant::now(),
        })
    }

    /// Build setup request (type 0) - establishes query context
    /// Must use transaction ID 0xfffffffe per protocol spec
    /// Format: Each header field is a NumberField/BinaryField with type tag
    fn build_setup_request(our_device_id: u8, _target_device_id: u8) -> Vec<u8> {
        let mut packet = Vec::with_capacity(64);

        // Start marker: NumberField(magic, 4) - 0x11 + 4 bytes
        packet.push(0x11);
        packet.extend_from_slice(&DB_MAGIC);

        // Transaction ID: NumberField(0xfffffffe, 4)
        packet.push(0x11);
        packet.extend_from_slice(&0xfffffffeu32.to_be_bytes());

        // Message type: NumberField(0, 2) - type 0 = SETUP_REQ
        packet.push(0x10);
        packet.extend_from_slice(&0u16.to_be_bytes());

        // Argument count: NumberField(1, 1)
        packet.push(0x0f);
        packet.push(0x01);

        // Argument types: BinaryField - length MUST match argc (1 byte for 1 arg)
        // Per dysentery: 0x06=number, 0x02=string, 0x03=blob
        packet.push(0x14);
        packet.extend_from_slice(&1u32.to_be_bytes());
        packet.extend_from_slice(&[0x06]);

        // Arg 1: NumberField(device_id, 4)
        packet.push(0x11);
        packet.extend_from_slice(&(our_device_id as u32).to_be_bytes());

        packet
    }

    /// Build metadata request (type 0x2002) - request track info availability
    /// Format: Each header field wrapped in NumberField/BinaryField with type tag
    /// Args: [menu_field, track_id] where menu_field = [our_device, 1, slot, track_type]
    /// NOTE: menu_field must use OUR announced device ID - CDJ validates this!
    fn build_metadata_request(our_device_id: u8, _source_device_id: u8, slot: u8, track_id: u32) -> Vec<u8> {
        let mut packet = Vec::with_capacity(60);

        // Start marker: NumberField(magic, 4)
        packet.push(0x11);
        packet.extend_from_slice(&DB_MAGIC);

        // Transaction ID: NumberField(2, 4)
        packet.push(0x11);
        packet.extend_from_slice(&2u32.to_be_bytes());

        // Message type: NumberField(0x2002, 2)
        packet.push(0x10);
        packet.extend_from_slice(&0x2002u16.to_be_bytes());

        // Argument count: NumberField(2, 1) - only 2 args!
        packet.push(0x0f);
        packet.push(0x02);

        // Argument types: BinaryField - length MUST match argc (2 bytes for 2 args)
        packet.push(0x14);
        packet.extend_from_slice(&2u32.to_be_bytes());
        packet.extend_from_slice(&[0x06, 0x06]);

        // Arg 1: DMST = [D=our_device_id, M=01, S=slot, T=01]
        // D = our virtual player number (the requester)
        // M = 0x01 for metadata menu
        // S = slot
        // T = 0x01 for rekordbox track
        let dmst = [our_device_id, 0x01, slot, 0x01];
        packet.push(0x11);
        packet.extend_from_slice(&dmst);

        // Arg 2: track_id (4-byte rekordbox ID)
        packet.push(0x11);
        packet.extend_from_slice(&track_id.to_be_bytes());

        packet
    }

    /// Build render request (type 0x3000) - fetch actual metadata items
    /// Per dysentery: args are [menu_field, offset, count, 0, total, 0]
    /// The menu was already created in Step 3 with the track_id
    fn build_render_request(our_device_id: u8, _source_device_id: u8, slot: u8, _track_id: u32, count: u32, total: u32, txid: u32) -> Vec<u8> {
        let mut packet = Vec::with_capacity(100);

        // Start marker: NumberField(magic, 4)
        packet.push(0x11);
        packet.extend_from_slice(&DB_MAGIC);

        // Transaction ID: NumberField(txid, 4) - incrementing for CDJ-3000 compatibility
        packet.push(0x11);
        packet.extend_from_slice(&txid.to_be_bytes());

        // Message type: NumberField(0x3000, 2)
        packet.push(0x10);
        packet.extend_from_slice(&0x3000u16.to_be_bytes());

        // Argument count: NumberField(6, 1)
        packet.push(0x0f);
        packet.push(0x06);

        // Argument types: BinaryField - length MUST match argc (6 bytes for 6 args)
        packet.push(0x14);
        packet.extend_from_slice(&6u32.to_be_bytes());
        packet.extend_from_slice(&[0x06, 0x06, 0x06, 0x06, 0x06, 0x06]);

        // Arg 1: DMST (menu_field) = [D=our_device_id, M=01, S=slot, T=01]
        // D = our virtual player number (the requester)
        // M = 0x01 for metadata menu
        // S = slot
        // T = 0x01 for rekordbox track
        let dmst = [our_device_id, 0x01, slot, 0x01];
        packet.push(0x11);
        packet.extend_from_slice(&dmst);

        // Arg 2: NumberField - offset = 0 (start from beginning)
        packet.push(0x11);
        packet.extend_from_slice(&0u32.to_be_bytes());

        // Arg 3: NumberField - count (how many items to fetch, max 64)
        packet.push(0x11);
        packet.extend_from_slice(&count.min(64).to_be_bytes());

        // Arg 4: NumberField - unknown = 0
        packet.push(0x11);
        packet.extend_from_slice(&0u32.to_be_bytes());

        // Arg 5: NumberField - total (total available items from Step 3)
        packet.push(0x11);
        packet.extend_from_slice(&total.to_be_bytes());

        // Arg 6: NumberField - unknown = 0
        packet.push(0x11);
        packet.extend_from_slice(&0u32.to_be_bytes());

        packet
    }

    /// Parse render response from dbserver - state machine for streamed responses
    /// Protocol: 0x4001 header, 0x4101 rows (with metadata), 0x4201 footer
    /// Uses timeout streak (2-3 timeouts) not single timeout to handle bursty data
    fn parse_render_response(stream: &mut TcpStream) -> Option<TrackMetadata> {
        let mut title = String::new();
        let mut artist = String::new();

        // State machine for parsing streamed render responses
        #[derive(Debug, Clone, Copy, PartialEq)]
        enum ParseState {
            WaitingForData,
            ReceivedHeader,   // Got 0x4001
            ReceivingRows,    // Getting 0x4101 rows
            Complete,         // Got 0x4201 footer or found enough data
        }

        let mut state = ParseState::WaitingForData;
        let mut all_data = Vec::with_capacity(16 * 1024);
        let mut consecutive_timeouts = 0;
        const MAX_TIMEOUT_STREAK: u32 = 3;  // Break after 3 consecutive timeouts
        const READ_TIMEOUT_MS: u64 = 350;   // Per-read timeout

        stream.set_read_timeout(Some(Duration::from_millis(READ_TIMEOUT_MS))).ok();

        let start = Instant::now();
        let max_duration = Duration::from_millis(2000);  // Overall timeout

        while start.elapsed() < max_duration && all_data.len() < 64 * 1024 && state != ParseState::Complete {
            let mut buf = [0u8; 4096];
            match stream.read(&mut buf) {
                Ok(0) => {
                    log::debug!("[ProDjLink] TCP: EOF");
                    break; // EOF
                }
                Ok(n) => {
                    consecutive_timeouts = 0;  // Reset timeout streak
                    all_data.extend_from_slice(&buf[..n]);

                    // Parse incrementally to detect message types
                    let data_len = all_data.len();

                    // Check for message type in header (bytes 11-12)
                    if data_len >= 13 && state == ParseState::WaitingForData {
                        let msg_type = u16::from_be_bytes([all_data[11], all_data[12]]);
                        if msg_type == 0x4001 {
                            state = ParseState::ReceivedHeader;
                            log::debug!("[ProDjLink] TCP: Got header 0x4001");
                        }
                    }

                    // Scan for message types in the buffer
                    // Look for 0x4101 (row) or 0x4201 (footer)
                    if data_len > 13 {
                        // Simple scan for footer marker
                        for i in 0..data_len.saturating_sub(2) {
                            if all_data[i] == 0x42 && all_data[i + 1] == 0x01 {
                                // Found footer, we're done
                                state = ParseState::Complete;
                                log::debug!("[ProDjLink] TCP: Got footer 0x4201 at offset {}", i);
                                break;
                            }
                        }
                        // Also check for 0x4101 rows
                        if state == ParseState::ReceivedHeader {
                            for i in 0..data_len.saturating_sub(2) {
                                if all_data[i] == 0x41 && all_data[i + 1] == 0x01 {
                                    state = ParseState::ReceivingRows;
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
                       || e.kind() == std::io::ErrorKind::TimedOut => {
                    consecutive_timeouts += 1;
                    log::trace!("[ProDjLink] TCP: timeout {} of {}", consecutive_timeouts, MAX_TIMEOUT_STREAK);

                    // If we have data and hit timeout streak, we're probably done
                    if consecutive_timeouts >= MAX_TIMEOUT_STREAK && !all_data.is_empty() {
                        log::debug!("[ProDjLink] TCP: Timeout streak reached with {} bytes, processing", all_data.len());
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    log::debug!("[ProDjLink] TCP: Error: {}", e);
                    break;
                }
            }
        }

        if all_data.is_empty() {
            log::warn!("[ProDjLink] No render response data");
            return None;
        }

        let total_read = all_data.len();

        // Log framing info + first 64 bytes (not full dump)
        if total_read >= 15 {
            let txid = u32::from_be_bytes([all_data[6], all_data[7], all_data[8], all_data[9]]);
            let msg_type = u16::from_be_bytes([all_data[11], all_data[12]]);
            let argc = if total_read > 14 { all_data[14] } else { 0 };
            log::info!("[ProDjLink] TCP render: {} bytes, type=0x{:04x}, txid={}, argc={}, state={:?}",
                total_read, msg_type, txid, argc, state);
            log::debug!("[ProDjLink] TCP first 64 bytes: {:02x?}", &all_data[..total_read.min(64)]);
        } else {
            log::info!("[ProDjLink] TCP render: {} bytes (too short for header)", total_read);
        }

        // Scan through all data looking for UTF-16BE strings (0x26 prefix)
        // StringField format: 0x26 + 4-byte length (in UTF-16 chars, not bytes!) + UTF-16BE data
        let mut i = 0;
        while i < total_read.saturating_sub(6) {
            if all_data[i] == 0x26 {
                if i + 5 > total_read {
                    break;
                }
                // Length is in UTF-16 character count (including null terminator)
                let char_len = u32::from_be_bytes([
                    all_data[i + 1],
                    all_data[i + 2],
                    all_data[i + 3],
                    all_data[i + 4],
                ]) as usize;

                // Convert character count to byte count (UTF-16 = 2 bytes per char)
                let byte_len = char_len * 2;

                if char_len > 0 && byte_len < 1024 && i + 5 + byte_len <= total_read {
                    let utf16_data = &all_data[i + 5..i + 5 + byte_len];

                    // Convert UTF-16BE to String
                    let mut utf16_vec = Vec::new();
                    for chunk in utf16_data.chunks(2) {
                        if chunk.len() == 2 {
                            utf16_vec.push(u16::from_be_bytes([chunk[0], chunk[1]]));
                        }
                    }

                    if let Ok(s) = String::from_utf16(&utf16_vec) {
                        let s = s.trim_matches('\0').trim().to_string();
                        if !s.is_empty() {
                            log::debug!("[ProDjLink] Found string (len={}): \"{}\"", char_len, s);
                            // First non-empty string is typically title, second is artist
                            if title.is_empty() {
                                title = s;
                            } else if artist.is_empty() {
                                artist = s;
                            }
                        }
                    }
                    i += 5 + byte_len;
                    continue;
                }
            }
            i += 1;
        }

        if title.is_empty() {
            log::warn!("[ProDjLink] No title found in {} bytes of render data", total_read);
            return None;
        }

        log::info!("[ProDjLink] Parsed metadata: \"{}\" by \"{}\"", title, artist);

        Some(TrackMetadata {
            title,
            artist: if artist.is_empty() {
                "Unknown Artist".to_string()
            } else {
                artist
            },
            album: None,
            duration: None,
            fetched_at: Instant::now(),
        })
    }

    /// Start a background metadata fetch for a track
    /// Includes debouncing: waits 300ms after track change to avoid mid-load fetches
    fn start_metadata_fetch(&self, device_ip: IpAddr, key: MetadataKey, source_device_id: u8) {
        // Debounce delay in ms - CDJs can be mid-load when we first see track change
        const DEBOUNCE_DELAY_MS: u64 = 300;

        {
            let state = self.state.lock();
            if state.pending_fetches.contains(&key) || state.metadata_cache.contains_key(&key) {
                return;
            }

            // Check debounce: don't fetch if track changed too recently
            if let Some(change_time) = state.last_track_change.get(&source_device_id) {
                if change_time.elapsed() < Duration::from_millis(DEBOUNCE_DELAY_MS) {
                    log::trace!("[ProDjLink] Debouncing metadata fetch for device {} ({}ms since change)",
                        source_device_id, change_time.elapsed().as_millis());
                    return;
                }
            }
        }

        // Insert pending after debounce check
        {
            let mut state = self.state.lock();
            state.pending_fetches.insert(key.clone());
        }

        let state = self.state.clone();
        let key_clone = key.clone();

        thread::Builder::new()
            .name(format!("prodjlink-metadata-{}", key.track_id))
            .spawn(move || {
                log::debug!(
                    "[ProDjLink] Fetching metadata for track {} from {}",
                    key.track_id, device_ip
                );

                let metadata =
                    Self::fetch_metadata(device_ip, key.device_id, key.slot, key.track_id, &state);

                let mut state = state.lock();
                state.pending_fetches.remove(&key_clone);

                if let Some(m) = metadata {
                    log::info!(
                        "[ProDjLink] Got metadata: \"{}\" by {}",
                        m.title,
                        m.artist
                    );
                    // Limit cache size
                    if state.metadata_cache.len() >= 500 {
                        // Remove oldest entry
                        if let Some(oldest_key) = state
                            .metadata_cache
                            .iter()
                            .min_by_key(|(_, v)| v.fetched_at)
                            .map(|(k, _)| k.clone())
                        {
                            state.metadata_cache.remove(&oldest_key);
                        }
                    }
                    state.metadata_cache.insert(key_clone, m);
                } else {
                    log::debug!(
                        "[ProDjLink] No metadata found for track {}",
                        key_clone.track_id
                    );
                }
            })
            .ok();
    }
}

impl Drop for ProDjLinkClient {
    fn drop(&mut self) {
        log::info!("[ProDjLink] Shutting down...");
        self.running.store(false, Ordering::Relaxed);
        if let Some(thread) = self.listener_thread.take() {
            let _ = thread.join();
        }
    }
}


impl ProDjLinkClient {
    /// Source name used by REACT and other integrations.
    pub fn name(&self) -> &str {
        "Pioneer CDJ"
    }

    /// Return the current master deck track, if a master deck has a track loaded.
    pub fn current_track(&mut self) -> Option<TrackInfo> {
        let state = self.state.lock();

        let master = state
            .devices
            .values()
            .find(|d| d.status.is_master && d.status.track_loaded)?;

        let device_id = master.device_id;
        let status = master.status.clone();
        let device_name = master.name.clone();

        let track_source_ip = state
            .devices
            .get(&status.track_device_id)
            .map(|d| d.ip)
            .unwrap_or(master.ip);

        let key = MetadataKey {
            device_id: status.track_device_id,
            slot: status.track_slot,
            track_id: status.track_id,
        };

        let metadata = state.metadata_cache.get(&key).cloned();
        let is_pending = state.pending_fetches.contains(&key);

        drop(state);

        if metadata.is_none() && !is_pending && status.track_id != 0 {
            self.start_metadata_fetch(track_source_ip, key, device_id);
        }

        Some(TrackInfo {
            title: metadata
                .as_ref()
                .map(|m| m.title.clone())
                .unwrap_or_else(|| format!("Track {}", status.track_id)),
            artist: metadata
                .as_ref()
                .map(|m| m.artist.clone())
                .unwrap_or(device_name),
            album: metadata.as_ref().and_then(|m| m.album.clone()),
            duration: metadata.as_ref().and_then(|m| m.duration),
            position: None,
            is_playing: status.is_playing,
            bpm: status.bpm,
            source: "Pioneer CDJ".to_string(),
            deck: Some(device_id),
            artwork: None,
        })
    }

    /// Check whether any CDJ devices are currently visible.
    pub fn is_available(&self) -> bool {
        !self.state.lock().devices.is_empty()
    }

    /// Get CDJ device info.
    pub fn cdj_devices(&self) -> Vec<CdjDeviceInfo> {
        self.get_device_info()
    }
}

impl Default for ProDjLinkClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ProDjLinkClient {
    /// Get a status message for UI display
    pub fn get_status_message(&self) -> String {
        let state = self.state.lock();

        if let Some(ref error) = state.last_error {
            return format!("CDJ: {}", error);
        }

        let device_count = state.devices.len();
        if device_count == 0 {
            "CDJ: Scanning network...".to_string()
        } else {
            let names: Vec<String> = state.devices.values()
                .map(|d| {
                    let mut flags = String::new();
                    if d.status.is_master { flags.push_str("[M]"); }
                    if d.status.is_playing { flags.push_str("[play]"); }
                    if d.status.is_looping { flags.push_str("[loop]"); }
                    if flags.is_empty() {
                        d.name.clone()
                    } else {
                        format!("{} {}", d.name, flags)
                    }
                })
                .collect();
            format!("CDJ: {} ({})", device_count, names.join(", "))
        }
    }

    /// Get list of discovered devices
    pub fn get_devices(&self) -> Vec<(u8, String, bool)> {
        let state = self.state.lock();
        state.devices.values()
            .map(|d| (d.device_id, d.name.clone(), d.status.is_master))
            .collect()
    }

    /// Check if any devices are connected
    pub fn has_devices(&self) -> bool {
        !self.state.lock().devices.is_empty()
    }

    /// Get full device info for all connected CDJs (with track metadata if available)
    /// Includes our virtual REACT device so users can see what ID we're using
    pub fn get_device_info(&self) -> Vec<CdjDeviceInfo> {
        let state = self.state.lock();
        let our_id = state.our_device_id;

        // Add our virtual device (REACT) first so it appears in the list
        let mut devices: Vec<CdjDeviceInfo> = vec![CdjDeviceInfo {
            device_id: our_id,
            name: format!("REACT (ID {})", our_id),
            ip: "127.0.0.1".parse().unwrap(),
            track_loaded: false,
            is_playing: false,
            is_looping: false,
            is_master: false,
            is_on_air: false,
            bpm: None,
            track_title: None,
            track_artist: None,
        }];

        // Collect device info with metadata lookups (exclude our own device)
        devices.extend(state.devices.values()
            .filter(|d| d.device_id != our_id)
            .map(|d| {
                // Look up track metadata from cache
                let key = MetadataKey {
                    device_id: d.status.track_device_id,
                    slot: d.status.track_slot,
                    track_id: d.status.track_id,
                };
                let metadata = state.metadata_cache.get(&key);

                CdjDeviceInfo {
                    device_id: d.device_id,
                    name: d.name.clone(),
                    ip: d.ip,
                    track_loaded: d.status.track_loaded,
                    is_playing: d.status.is_playing,
                    is_looping: d.status.is_looping,
                    is_master: d.status.is_master,
                    is_on_air: d.status.is_on_air,
                    bpm: d.status.bpm,
                    track_title: metadata.map(|m| m.title.clone()),
                    track_artist: metadata.map(|m| m.artist.clone()),
                }
            })
        );

        // Collect pending fetches info
        let pending_fetches: std::collections::HashSet<MetadataKey> =
            state.pending_fetches.clone();

        // Collect devices that need metadata fetches
        // Use the track source device's IP, not the playing device's IP
        // Include playing device_id for debounce tracking
        let devices_needing_fetch: Vec<(IpAddr, MetadataKey, u8)> = state.devices.values()
            .filter(|d| d.status.track_loaded && d.status.track_id != 0)
            .filter_map(|d| {
                let key = MetadataKey {
                    device_id: d.status.track_device_id,
                    slot: d.status.track_slot,
                    track_id: d.status.track_id,
                };
                if !state.metadata_cache.contains_key(&key) && !pending_fetches.contains(&key) {
                    // Get IP of the device that owns the media (track_device_id)
                    let source_ip = state.devices.get(&d.status.track_device_id)
                        .map(|src| src.ip)
                        .unwrap_or(d.ip); // Fallback to playing device
                    Some((source_ip, key, d.device_id))  // Include playing device ID for debounce
                } else {
                    None
                }
            })
            .collect();

        drop(state); // Release lock before network calls

        // Start metadata fetches for all loaded tracks (not just master)
        for (device_ip, key, playing_device_id) in devices_needing_fetch {
            self.start_metadata_fetch(device_ip, key, playing_device_id);
        }

        // Sort by device ID for consistent display
        devices.sort_by_key(|d| d.device_id);
        devices
    }

    /// Get all track changes since last drain (for database sync)
    /// This returns and clears the track history - uses separate lock for non-blocking
    pub fn drain_track_history(&self) -> Vec<TrackChangeRecord> {
        std::mem::take(&mut *self.track_history.lock())
    }

    /// Get track history without clearing (for inspection)
    pub fn get_track_history(&self) -> Vec<TrackChangeRecord> {
        self.track_history.lock().clone()
    }

    /// Update track metadata in history records when metadata becomes available
    /// Call this after metadata fetch completes to update any pending records
    pub fn update_history_metadata(&self, track_id: u32, title: &str, artist: &str) {
        let mut history = self.track_history.lock();
        for record in history.iter_mut() {
            if record.track_id == track_id && record.title.is_none() {
                record.title = Some(title.to_string());
                record.artist = Some(artist.to_string());
                log::info!(
                    "[ProDjLink] Updated history record for track {}: \"{}\" by \"{}\"",
                    track_id, title, artist
                );
            }
        }
    }
}

/// Backward-compatible alias for code that used the REACT-internal name.
pub type ProDjLinkSource = ProDjLinkClient;
