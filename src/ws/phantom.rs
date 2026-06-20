use actix_web::{web, HttpRequest, HttpResponse};
use actix_ws::Message;
use std::sync::Arc;
use tokio::sync::broadcast;

// ── Global broadcast channel — all connected HomeScreen users receive events ──
pub type PhantomTx = Arc<broadcast::Sender<String>>;

pub fn create_phantom_channel() -> PhantomTx {
    let (tx, _) = broadcast::channel(256);
    Arc::new(tx)
}

pub async fn phantom_ws(
    req: HttpRequest,
    stream: web::Payload,
    tx: web::Data<PhantomTx>,
) -> Result<HttpResponse, actix_web::Error> {
    let (res, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;

    let mut rx = tx.subscribe();

    actix_web::rt::spawn(async move {
        loop {
            tokio::select! {
                // ── Broadcast received → forward to this client ───────────────
                Ok(msg) = rx.recv() => {
                    if session.text(msg).await.is_err() {
                        break;
                    }
                }
                // ── Client message (ping/close) ───────────────────────────────
                Some(Ok(msg)) = msg_stream.recv() => {
                    match msg {
                        Message::Ping(bytes) => {
                            if session.pong(&bytes).await.is_err() { break; }
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
                else => break,
            }
        }
        let _ = session.close(None).await;
    });

    Ok(res)
}