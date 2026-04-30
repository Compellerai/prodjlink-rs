# prodjlink-rs

Rust client for monitoring Pioneer/AlphaTheta Pro DJ Link networks.

Compeller built this because REACT needed reliable native Pro DJ Link awareness for live visuals. We are busy building Compeller, but this utility should be useful to other Rust, DJ, lighting, and show-control projects, so we are sharing it.

## Current scope

- Join a Pro DJ Link LAN as a virtual player.
- Listen for CDJ status on UDP 50002.
- Send keep-alive/status packets on UDP 50000/50002 so players answer us.
- Track discovered decks, master/play/loop/on-air flags, BPM, source slot, and rekordbox track id.
- Fetch metadata from the CDJ dbserver over TCP when available.
- Expose simple device and current-master-track APIs.

## Not included

- REACT UI.
- SQLite persistence.
- Audio analysis.
- Lighting/DMX output.
- Internet or SaaS calls.

## Quick start

Add the crate:

```toml
prodjlink-rs = "0.1"
```

Scan a Pro DJ Link network:

```bash
cargo run --example scan
```

Minimal use:

```rust
use std::{thread, time::Duration};
use prodjlink_rs::ProDjLinkClient;

fn main() {
    let mut client = ProDjLinkClient::new();

    loop {
        for device in client.cdj_devices() {
            println!("deck {}: {} playing={} master={} bpm={:?}",
                device.device_id,
                device.name,
                device.is_playing,
                device.is_master,
                device.bpm,
            );
        }

        if let Some(track) = client.current_track() {
            println!("master: {} - {} bpm={:?}", track.artist, track.title, track.bpm);
        }

        thread::sleep(Duration::from_secs(2));
    }
}
```

## Hardware tested

Known working from Compeller/REACT testing:

- CDJ-3000
- DJS-1000
- XDJ-1000MK2

Hardware reports are welcome, especially for CDJ-2000NXS2, additional XDJ units, DJM mixers, and mixed-device Pro DJ Link networks.

## Safety note

This crate sends local LAN broadcast packets and may bind UDP ports 50000 and 50002. Do not run it on a production DJ network unless you understand Pro DJ Link behavior and have operator approval.

The crate does not call Compeller services and does not use the internet. It only talks on the local Pro DJ Link network.

## Protocol attribution

This implementation is informed by public Pro DJ Link research and documentation, especially Deep Symmetry's DJL Analysis docs:

https://djl-analysis.deepsymmetry.org/

This project is not affiliated with, endorsed by, or sponsored by Pioneer DJ or AlphaTheta.

## Contributing

We are especially interested in:

- packet fixtures from more hardware
- hardware compatibility reports
- metadata parser improvements
- a JSON event-stream example
- an async/Tokio API
- troubleshooting docs for real DJ networks

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT. See [LICENSE](LICENSE).
