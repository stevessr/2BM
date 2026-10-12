use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    Key, XChaCha20Poly1305, XNonce,
};
use rand::RngCore;
use serde_json::Value;

pub fn encrypt(key: &[u8; 32], credentials: &Value) -> Result<String> {
    let mut nonce = [0u8; 24];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let cipher = XChaCha20Poly1305::new(Key::from_slice(key));
    let plaintext = serde_json::to_vec(credentials)?;
    let ciphertext = cipher.encrypt(XNonce::from_slice(&nonce), plaintext.as_ref())
        .map_err(|_| anyhow::anyhow!("credential encryption failed"))?;
    let mut data = Vec::with_capacity(24 + ciphertext.len());
    data.extend_from_slice(&nonce);
    data.extend_from_slice(&ciphertext);
    Ok(STANDARD_NO_PAD.encode(data))
}

pub fn decrypt(key: &[u8; 32], encoded: &str) -> Result<Value> {
    let data = STANDARD_NO_PAD.decode(encoded).context("invalid credential encoding")?;
    if data.len() < 40 {
        bail!("invalid credential payload");
    }
    let cipher = XChaCha20Poly1305::new(Key::from_slice(key));
    let plaintext = cipher.decrypt(XNonce::from_slice(&data[..24]), &data[24..])
        .map_err(|_| anyhow::anyhow!("credential decryption failed"))?;
    Ok(serde_json::from_slice(&plaintext)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn encrypted_credentials_roundtrip() {
        let key = [7u8; 32];
        let input = serde_json::json!({"token": "top-secret"});
        let sealed = encrypt(&key, &input).unwrap();
        assert!(!sealed.contains("top-secret"));
        assert_eq!(decrypt(&key, &sealed).unwrap(), input);
        assert!(decrypt(&[8u8; 32], &sealed).is_err());
    }
}
