use crate::error::TypesError;
use crate::leb128::read_leb128;
use bytes::Buf;

// BigBrotr patch: validate declared metadata lengths before copying any bytes.
pub(crate) fn read_string_bounded(mut buffer: impl Buf, maximum: usize) -> Result<String, TypesError> {
    let length = read_leb128(&mut buffer)?;
    if length == 0 || length > maximum as u64 {
        return Err(TypesError::TypeParsingError("metadata string length outside bounded profile".into()));
    }
    let length = usize::try_from(length)
        .map_err(|_| TypesError::TypeParsingError("metadata string length exceeds usize".into()))?;
    ensure_size(&mut buffer, length)?;
    String::from_utf8(buffer.copy_to_bytes(length).to_vec())
        .map_err(|_| TypesError::TypeParsingError("metadata string is not UTF-8".into()))
}

#[inline]
pub(crate) fn read_string(mut buffer: impl Buf) -> Result<String, TypesError> {
    let length = usize::try_from(read_leb128(&mut buffer)?)
        .map_err(|_| TypesError::TypeParsingError("string length exceeds usize".into()))?;
    if length == 0 {
        return Ok("".to_string());
    }
    ensure_size(&mut buffer, length)?;
    let result = String::from_utf8_lossy(&buffer.copy_to_bytes(length)).to_string();
    Ok(result)
}

#[inline]
pub(crate) fn ensure_size(buffer: impl Buf, size: usize) -> Result<(), TypesError> {
    if buffer.remaining() < size {
        Err(TypesError::NotEnoughData(format!(
            "expected at least {} bytes, but only {} bytes remaining",
            size,
            buffer.remaining()
        )))
    } else {
        Ok(())
    }
}
