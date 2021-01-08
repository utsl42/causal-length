use super::*;
use std::cmp::{max, Ord};
use std::collections::HashMap;
use std::hash::Hash;

/// Set implements the set described in the paper, with the addition of a tag. Set only uses the
/// tag for garbage collection of old removed members.
#[derive(Clone, Debug)]
pub struct Set<T, Tag, CL>
where
    T: Eq + Hash + Clone,
    Tag: Ord + Copy,
    CL: CausalLength,
{
    // HashMap, because the "set" needs to allow mutating the time and causal length.
    map: HashMap<T, (Tag, CL)>,
}

impl<T, Tag, CL> Set<T, Tag, CL>
where
    T: Eq + Hash + Clone,
    Tag: Ord + Copy,
    CL: CausalLength,
{
    fn new() -> Set<T, Tag, CL> {
        Set {
            map: HashMap::new(),
        }
    }

    fn get(&self, member: &T) -> Option<Tag> {
        if let Some(e) = self.map.get(member).to_owned() {
            if e.1.is_odd() {
                return Some(e.0);
            }
        }
        None
    }

    fn contains(&self, member: &T) -> bool {
        self.get(member).is_some()
    }

    fn add(&mut self, member: T, tag: Tag) {
        let one: CL = CL::one();
        let mut e = self.map.entry(member).or_insert((tag, one));
        // s{e |-> s(e)+1} if even
        //s if odd s(e)
        if e.1.is_even() {
            e.1 = e.1 + one;
        }
        // always use the max value of timestamp
        e.0 = max(e.0, tag);
    }

    fn remove(&mut self, member: T, tag: Tag) {
        self.map.entry(member).and_modify(|e| {
            // {} if even(s(e))
            // { e |-> s(e) + 1 } if odd(s(e))
            if e.1.is_odd() {
                e.1 = e.1 + CL::one()
            }
            e.0 = max(e.0, tag);
        });
        // ignore attempts to remove items that aren't present...
    }

    fn iter(&self) -> impl Iterator<Item = (&T, Tag)> + '_ {
        self.map
            .iter()
            .filter(|(_k, v)| v.1.is_odd())
            .map(|(k, v)| (k, v.0))
    }

    fn delta_iter(&self) -> impl Iterator<Item = (&T, Tag, CL)> + '_ {
        self.map.iter().map(|(k, v)| (k, v.0, v.1))
    }

    fn merge_delta(&mut self, delta: (&T, Tag, CL), min_tag: Tag) {
        if delta.2.is_even() && delta.1 < min_tag {
            // ignore excessively old remove records
            return;
        }
        let item: T = delta.0.clone();
        self.map
            .entry(item)
            .and_modify(|e| {
                // (s⊔s′)(e) = max(s(e),s′(e))
                e.0 = max(e.0, delta.1);
                e.1 = max(e.1, delta.2);
            })
            .or_insert((delta.1, delta.2));
    }

    fn merge(&mut self, other: &Self, min_tag: Tag) {
        for delta in other.delta_iter() {
            self.merge_delta(delta, min_tag);
        }
    }

    fn retain(&mut self, min_tag: Tag) {
        self.map
            .retain(|_k, (tag, length)| length.is_odd() || min_tag < *tag);
    }
}

#[cfg(feature = "serialization")]
mod serialization {
    use super::*;
    use serde::de::{SeqAccess, Visitor};
    use serde::export::Formatter;
    use serde::ser::SerializeSeq;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::marker::PhantomData;

    impl<T, Tag, CL> Serialize for Set<T, Tag, CL>
    where
        T: Eq + Hash + Clone + Serialize,
        Tag: Ord + Copy + Serialize,
        CL: CausalLength + Serialize,
    {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(self.map.len()))?;
            for member in self.delta_iter() {
                seq.serialize_element(&member)?;
            }
            seq.end()
        }
    }

    struct DeltaVisitor<T, Tag, CL>(PhantomData<T>, PhantomData<Tag>, PhantomData<CL>);

    impl<'de, T, Tag, CL> Visitor<'de> for DeltaVisitor<T, Tag, CL>
    where
        T: Eq + Hash + Clone + Deserialize<'de>,
        Tag: Ord + Copy + Deserialize<'de>,
        CL: CausalLength + Deserialize<'de>,
    {
        type Value = HashMap<T, (Tag, CL)>;

        fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an integer between 0 and 2^64")
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut map: HashMap<T, (Tag, CL)> =
                HashMap::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(d) = seq.next_element::<(T, Tag, CL)>()? {
                map.insert(d.0, (d.1, d.2));
            }
            Ok(map)
        }
    }

    impl<'de, T, Tag, CL> Deserialize<'de> for Set<T, Tag, CL>
    where
        T: Eq + Hash + Clone + Deserialize<'de>,
        Tag: Ord + Copy + Deserialize<'de>,
        CL: CausalLength + Deserialize<'de>,
    {
        fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let visitor = DeltaVisitor::<T, Tag, CL>(PhantomData, PhantomData, PhantomData);
            let map = deserializer.deserialize_seq(visitor)?;

            Ok(Set { map })
        }
    }
}

#[cfg(feature = "serialization")]
pub use serialization::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_add() {
        let later_time = 1;
        let mut cls: Set<(u32, u32), u32, u16> = Set::new();

        cls.add((1, 2), later_time);
        cls.add((1, 2), later_time);
        cls.add((1, 2), later_time);
        assert_eq!(cls.map.len(), 1);
        assert_eq!(cls.map.get(&(1, 2)), Some(&(later_time, 1)));
        assert_eq!(cls.contains(&(1, 2)), true);
        assert_eq!(cls.get(&(3, 4)), None);
    }

    #[test]
    fn test_remove() {
        let time_1 = 1;
        let time_2 = 2;
        let time_3 = 3;
        let mut cls: Set<(u32, u32), u32, u16> = Set::new();

        cls.add((1, 2), time_1);
        cls.add((2, 3), time_1);
        cls.remove((1, 2), time_2);
        cls.remove((2, 3), time_2);
        cls.add((2, 3), time_3);
        // check map
        assert_eq!(cls.map.len(), 2);
        assert_eq!(cls.map.get(&(2, 3)), Some(&(time_3, 3)));
        assert_eq!(cls.map.get(&(1, 2)), Some(&(time_2, 2)));
        // check edges
        let values: Vec<(&(u32, u32), u32)> = cls.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], (&(2, 3), time_3));
    }

    #[test]
    fn test_merge() {
        let time_0 = 0;
        let time_1 = 1;
        let time_2 = 2;
        let time_3 = 3;
        let mut cls1: Set<(u32, u32), u32, u16> = Set::new();
        let mut cls2: Set<(u32, u32), u32, u16> = Set::new();

        cls1.add((1, 2), time_1);
        cls1.add((2, 3), time_1);
        cls2.merge(&cls1, time_0);
        cls2.remove((1, 2), time_2);
        cls1.remove((1, 2), time_2);
        cls1.remove((2, 3), time_2);
        cls2.merge(&cls1, time_0);
        cls2.add((2, 3), time_3);
        // check map
        assert_eq!(cls2.map.len(), 2);
        assert_eq!(cls2.map.get(&(2, 3)), Some(&(time_3, 3)));
        assert_eq!(cls2.map.get(&(1, 2)), Some(&(time_2, 2)));
        // check edges
        let values: Vec<(&(u32, u32), u32)> = cls2.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], (&(2, 3), time_3));
    }

    #[test]
    fn test_retain() {
        let time_0 = 0;
        let time_1 = 1;
        let time_2 = 2;
        let time_3 = 3;
        let mut cls: Set<(u32, u32), u32, u16> = Set::new();

        cls.add((1, 2), time_0);
        cls.add((2, 3), time_0);
        cls.remove((1, 2), time_1);
        cls.remove((2, 3), time_1);
        cls.add((2, 3), time_2);
        // check map
        assert_eq!(cls.map.len(), 2);
        assert_eq!(cls.map.get(&(2, 3)), Some(&(time_2, 3)));
        assert_eq!(cls.map.get(&(1, 2)), Some(&(time_1, 2)));
        // check edges
        let values: Vec<(&(u32, u32), u32)> = cls.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], (&(2, 3), time_2));
        // now clear old removes
        cls.retain(time_3);
        assert_eq!(cls.map.len(), 1);
        assert_eq!(cls.map.get(&(2, 3)), Some(&(time_2, 3)));
        // attempt to merge an out of date remove
        cls.merge_delta((&(2, 3), time_2, 2), time_0);
        assert_eq!(cls.map.len(), 1);
        assert_eq!(cls.map.get(&(2, 3)), Some(&(time_2, 3)));
    }

    #[cfg(feature = "serialization")]
    #[test]
    fn test_serialization() {
        let time_1 = 1;
        let time_2 = 2;
        let time_3 = 3;
        let mut cls: Set<(u32, u32), u32, u16> = Set::new();

        cls.add((1, 2), time_1);
        cls.add((2, 3), time_1);
        cls.remove((1, 2), time_2);
        cls.remove((2, 3), time_2);
        cls.add((2, 3), time_3);

        let data = serde_json::to_string(&cls).unwrap_or("".to_owned());
        let cls2: Set<(u32, u32), u32, u16> = serde_json::from_str(&data).unwrap();
        assert_eq!(cls.map, cls2.map);
    }
}
