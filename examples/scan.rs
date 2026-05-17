use std::thread;
use std::time::Duration;

use prodjlink_rs::ProDjLinkClient;

fn main() {
    env_logger::init();

    let mut client = ProDjLinkClient::new();
    println!("Scanning Pro DJ Link network. Press Ctrl-C to stop.");

    loop {
        println!("{}", client.get_status_message());

        for device in client.cdj_devices() {
            println!(
                "deck={} name={} ip={} loaded={} playing={} master={} on_air={} bpm={:?} title={:?} artist={:?}",
                device.device_id,
                device.name,
                device.ip,
                device.track_loaded,
                device.is_playing,
                device.is_master,
                device.is_on_air,
                device.bpm,
                device.track_title,
                device.track_artist
            );
        }

        if let Some(track) = client.current_track() {
            println!(
                "master_track={} - {} bpm={:?}",
                track.artist, track.title, track.bpm
            );
        }

        if let Some(beat) = client.master_beat() {
            println!(
                "master_beat deck={} bpm={:.2} beat_in_measure={} age_ms={}",
                beat.device_id,
                beat.bpm,
                beat.beat_in_measure,
                beat.last_beat_at.elapsed().as_millis()
            );
        }

        thread::sleep(Duration::from_secs(2));
    }
}
