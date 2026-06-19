//! The sidecar spawn + `FRONT` handshake (`quicmix::front`), exercised with a stub
//! command (no real substrate or daemon): proves the client spawns a sidecar, reads
//! its announced UDP front, and dials that address — and fails fast otherwise.

use quicmix::front::{self, SubstrateChoice};

#[tokio::test]
async fn direct_front_is_the_gateway() {
    let gateway = "203.0.113.7:4433".parse().unwrap();
    let front = front::resolve(&SubstrateChoice::Direct, gateway, None).await.unwrap();
    assert_eq!(front.addr, gateway);
}

#[tokio::test]
async fn spawns_sidecar_and_reads_announced_front() {
    // A stub "sidecar": prints some log noise, then the FRONT line, then stays alive.
    let choice = SubstrateChoice::SidecarCmd {
        name: "stub".into(),
        program: "sh".into(),
        args: vec![
            "-c".into(),
            "printf 'booting substrate...\\nFRONT 127.0.0.1:45999\\n'; sleep 3".into(),
        ],
    };
    let gateway = "203.0.113.7:4433".parse().unwrap();
    let front = front::resolve(&choice, gateway, None).await.expect("resolve sidecar front");
    assert_eq!(front.addr, "127.0.0.1:45999".parse().unwrap());
}

#[tokio::test]
async fn sidecar_that_never_announces_errors() {
    let choice = SubstrateChoice::SidecarCmd {
        name: "stub".into(),
        program: "sh".into(),
        args: vec!["-c".into(), "echo no front here; exit 0".into()],
    };
    let gateway = "203.0.113.7:4433".parse().unwrap();
    assert!(front::resolve(&choice, gateway, None).await.is_err());
}
