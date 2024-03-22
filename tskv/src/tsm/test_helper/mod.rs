mod tsm;

use minivec::MiniVec;
pub use tsm::*;

// pub use super::reader::test::read_and_check;
pub use super::tombstone::test::write_to_tsm_tombstone_v2;
// pub use super::writer::test::write_to_tsm;

fn string_into_mini_vec(str_data: String) -> MiniVec<u8> {
    let mut mv = MiniVec::with_capacity(str_data.len());
    mv.extend(str_data.as_bytes());
    mv
}

pub mod geometry {
    use minivec::MiniVec;

    use super::string_into_mini_vec;

    #[test]
    fn test_string_into_mini_vec() {
        let test_str = "test_str";
        let mv = string_into_mini_vec(test_str.to_string());
        assert_eq!(mv.as_slice(), test_str.as_bytes());
    }

    pub fn point(x: isize, y: isize) -> MiniVec<u8> {
        let str_data = format!("POINT({x} {y})");
        string_into_mini_vec(str_data)
    }

    pub fn line_string(x: isize, y: isize) -> MiniVec<u8> {
        let str_data = format!("LINESTRING({x} {y}, {} {})", x + 1, y + 1);
        string_into_mini_vec(str_data)
    }

    pub fn polygon_square(x: isize, y: isize) -> MiniVec<u8> {
        let str_data = format!(
            "POLYGON(({x} {y}, {x} {y_1}, {x_1} {y_1}, {x_1} {y}))",
            x = x,
            y = y,
            x_1 = x + 1,
            y_1 = y + 1,
        );
        string_into_mini_vec(str_data)
    }

    pub fn multi_point_cross(x: isize, y: isize) -> MiniVec<u8> {
        let str_data = format!(
            "MULTIPOINT(({x} {y}, {} {}), ({} {}, {} {}))",
            x + 1,
            y + 1,
            x,
            y + 1,
            x + 1,
            y
        );
        string_into_mini_vec(str_data)
    }

    pub fn multi_line_string_cross(x: isize, y: isize) -> MiniVec<u8> {
        let str_data = format!(
            "MULTILINESTRING(({x} {y} {} {}), ({} {} {} {}))",
            x + 1,
            y + 1,
            x,
            y + 1,
            x + 1,
            y
        );
        string_into_mini_vec(str_data)
    }

    pub fn multi_polygon_squares_vertical(x: isize, y: isize) -> MiniVec<u8> {
        let str_data = format!(
        "MULTIPOLYGON((({x} {y}, {x} {y_1}, {x_1} {y_1}, {x_1} {y})),(({x} {y_1}, {x} {y_2}, {x_1} {y_2}, {x_1} {y_1})))",
        x = x,
        y = y,
        x_1 = x + 1,
        y_1 = y + 1,
        y_2 = y + 2,
    );
        string_into_mini_vec(str_data)
    }

    pub fn geometry_collection_ok(x: isize, y: isize) -> MiniVec<u8> {
        let str_data = format!(
        "GEOMETRYCOLLECTION(POLYGON(({x} {y}, {x} {y_1}, {x_2} {y_1}, {x_2} {y})),LINESTRING({x_1} {y_2}, {x_1} {y_3}))",
        x = x,
        y = y,
        x_1 = x + 1,
        x_2 = x + 2,
        y_1 = y + 1,
        y_2 = y + 2,
        y_3 = y - 1,
    );
        string_into_mini_vec(str_data)
    }

    #[test]
    fn test_geometry_generator() {
        let point = point(1, 1).to_vec();
        let point_str = String::from_utf8(point).unwrap();
        assert_eq!(point_str, "POINT(1 1)");

        let line_string = line_string(1, 1).to_vec();
        let line_string_str = String::from_utf8(line_string).unwrap();
        assert_eq!(line_string_str, "LINESTRING(1 1, 2 2)");

        let polygon_square = polygon_square(1, 1).to_vec();
        let polygon_square_str = String::from_utf8(polygon_square).unwrap();
        assert_eq!(polygon_square_str, "POLYGON((1 1, 1 2, 2 2, 2 1))");

        let multi_point_cross = multi_point_cross(1, 1).to_vec();
        let multi_point_cross_str = String::from_utf8(multi_point_cross).unwrap();
        assert_eq!(multi_point_cross_str, "MULTIPOINT((1 1, 2 2), (1 2, 2 1))");

        let multi_polygon_squares_vertical = multi_polygon_squares_vertical(1, 1).to_vec();
        let multi_polygon_squares_vertical_str =
            String::from_utf8(multi_polygon_squares_vertical).unwrap();
        assert_eq!(
            multi_polygon_squares_vertical_str,
            "MULTIPOLYGON(((1 1, 1 2, 2 2, 2 1)),((1 2, 1 3, 2 3, 2 2)))"
        );

        let geometry_collection_ok = geometry_collection_ok(1, 1).to_vec();
        let geometry_collection_ok_str = String::from_utf8(geometry_collection_ok).unwrap();
        assert_eq!(
            geometry_collection_ok_str,
            "GEOMETRYCOLLECTION(POLYGON((1 1, 1 2, 3 2, 3 1)),LINESTRING(2 3, 2 0))"
        );
    }
}
