use super::*;
use std::cmp::max;

#[cfg(feature = "serialization")]
use serde_derive::{Deserialize, Serialize};

/// Causal Length Register
///
/// Register implements a single member for the set described in the paper, with the addition of a
/// tag. Sort of acts like a CRDT Option type. Register doesn't directly use the tag, but it also
/// acts as a delta for the other CRDT's in this crate.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
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

    // Accessor for tag
    pub fn item(&self) -> &T {
        &self.item
    }

    // Accessor for tag
    pub fn tag(&self) -> Tag {
        self.tag
    }

    // Accessor for length
    pub fn length(&self) -> CL {
        self.length
    }
}

impl<T, Tag, CL> Register<T, Tag, CL>
where
    T: Key + Ord,
    Tag: TagT,
    CL: CausalLength,
{
    /// Merge two register values
    pub fn merge(&mut self, other: &Register<T, Tag, CL>) {
        if other.length > self.length && other.length.is_odd() {
            self.item = other.item.clone();
            self.tag = other.tag;
        }
        if other.length == self.length {
            if other.tag > self.tag {
                self.item = other.item.clone();
                self.tag = max(self.tag, other.tag);
            } else if other.tag == self.tag && other.item > self.item {
                self.item = other.item.clone();
            }
        }
        self.length = max(self.length, other.length);
    }
}

#[cfg(test)]
use quickcheck::{Arbitrary, Gen};
#[cfg(test)]
impl<T, Tag, CL> Arbitrary for Register<T, Tag, CL>
where
    T: Key + Arbitrary,
    Tag: TagT + Arbitrary,
    CL: CausalLength + Arbitrary,
{
    fn arbitrary(g: &mut Gen) -> Register<T, Tag, CL> {
        Register::make(T::arbitrary(g), Tag::arbitrary(g), CL::arbitrary(g))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;

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
        let data = serde_json::to_string(&reg1).unwrap();
        assert_eq!(&data, r#"{"item":"foo","tag":0,"length":1}"#);
    }

    fn merge(mut acc: Register<u8, u8, u8>, el: &Register<u8, u8, u8>) -> Register<u8, u8, u8> {
        acc.merge(el);
        acc
    }

    #[quickcheck]
    fn is_merge_commutative(xs: Vec<Register<u8, u8, u8>>) -> bool {
        let left = xs.iter().fold(Register::default(), merge);
        let right = xs.iter().rfold(Register::default(), merge);
        left.get() == right.get()
    }

    #[test]
    fn test_fup() {
        let xs = vec![
            Register {
                item: 255,
                tag: 174,
                length: 1,
            },
            Register {
                item: 9,
                tag: 162,
                length: 176,
            },
        ];
        let left = xs.iter().fold(Register::default(), merge);
        let right = xs.iter().rfold(Register::default(), merge);
        assert_eq!(left.get(), right.get());
    }
}
