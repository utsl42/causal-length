use super::*;
use std::cmp::max;

#[cfg(feature = "serialization")]
use serde_derive::{Deserialize, Serialize};

/// Causal Length Register
///
/// Register implements a single member for the set described in the paper, with the addition of a
/// tag. Sort of acts like a CRDT Option type. Register doesn't directly use the tag, but it also
/// acts as a delta for the other CRDT's in this crate.
#[derive(Clone, Debug, Default, PartialEq)]
#[cfg_attr(feature = "serialization", derive(Serialize, Deserialize))]
pub struct Register<T, Tag, CL>
where
    T: Key,
    Tag: TagT,
    CL: CausalLength,
{
    pub(crate) item: T,
    pub(crate) tag: Tag,
    pub(crate) length: CL,
}

impl<T, Tag, CL> Register<T, Tag, CL>
where
    T: Key,
    Tag: TagT,
    CL: CausalLength,
{
    /// Create a new `Register`
    pub fn new(item: T, tag: Tag) -> Register<T, Tag, CL> {
        Register {
            item,
            tag,
            length: CL::one(),
        }
    }

    pub(crate) fn make<I>(item: I, tag: Tag, length: CL) -> Register<T, Tag, CL>
    where
        I: Into<T>,
    {
        Register {
            item: item.into(),
            tag,
            length,
        }
    }

    /// Returns `None` if the register is empty. If present returns `Some(&T, Tag)`
    pub fn get(&self) -> Option<(&T, Tag)> {
        match &self {
            Register { item, tag, length } if length.is_odd() => Some((item, *tag)),
            _ => None,
        }
    }

    /// Set value
    pub fn set(&mut self, item: T, tag: Tag) {
        self.item = item;
        self.tag = max(self.tag, tag);

        if self.length.is_odd() {
            self.length = self.length + CL::one() + CL::one();
        } else {
            self.length = self.length + CL::one();
        }
    }

    /// Clear value
    pub fn clear(&mut self, tag: Tag) {
        if self.length.is_odd() {
            self.length = self.length + CL::one();
            self.tag = max(self.tag, tag);
        }
    }

    /// Merge two register values
    pub fn merge(&mut self, other: &Register<T, Tag, CL>) {
        if other.length > self.length {
            if other.length.is_odd() && other.tag > self.tag {
                self.item = other.item.clone();
            }
            self.length = max(self.length, other.length);
            self.tag = max(self.tag, other.tag);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "serialization")]
    use serde_json;

    #[test]
    fn test_merge() {
        let mut reg1: Register<&str, u32, u16> = Register::new("foo", 0);
        let mut reg2 = reg1.clone();
        let mut reg3 = reg1.clone();

        reg3.clear(2);
        reg2.set("bar", 2);

        reg1.merge(&reg2);
        reg1.merge(&reg3);

        assert_eq!(reg1, reg2);
        assert_eq!(reg1.length, 3);
        assert_eq!(reg1.get(), Some((&"bar", 2)));
    }

    #[cfg(feature = "serialization")]
    #[test]
    fn test_serialization() {
        let reg1: Register<&str, u32, u16> = Register::new("foo", 0);
        let data = serde_json::to_string(&reg1).unwrap_or("".to_owned());
        assert_eq!(data, r#"{"item":"foo","tag":0,"length":1}"#.to_owned());
    }
}
