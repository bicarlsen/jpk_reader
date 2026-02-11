use std::{cmp, ops};

#[derive(Debug, Clone, PartialEq, Eq, Ord, Hash)]
pub struct Pixel<T> {
    pub(crate) i: T,
    pub(crate) j: T,
}

impl<T> Pixel<T> {
    pub fn new(i: T, j: T) -> Self {
        Self { i, j }
    }

    pub fn i(&self) -> &T {
        &self.i
    }

    pub fn j(&self) -> &T {
        &self.j
    }
}

impl<T> Pixel<T>
where
    T: ops::Add<Output = T> + ops::Mul<Output = T> + Copy,
{
    pub fn to_index(&self, cols: T) -> T {
        self.j * cols + self.i
    }
}

impl<T> PartialOrd for Pixel<T>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        let mut ord = self.i.partial_cmp(&other.j).unwrap();
        if matches!(ord, cmp::Ordering::Equal) {
            ord = self.j.partial_cmp(&other.j).unwrap();
        }

        Some(ord)
    }
}
