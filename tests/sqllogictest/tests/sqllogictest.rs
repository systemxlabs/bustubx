use bustubx_sqllogictest::BustubxDB;
use std::path::{Path, PathBuf};

#[test]
fn sqllogictest() {
    let test_files = read_dir_recursive("slt/");
    println!("test_files: {:?}", test_files);

    for file in test_files {
        let db = BustubxDB::new();
        let mut tester = sqllogictest::Runner::new(db);
        println!(
            "======== start to run file {} ========",
            file.to_str().unwrap()
        );
        tester.run_file(file).unwrap();
    }
}

fn read_dir_recursive<P: AsRef<Path>>(path: P) -> Vec<PathBuf> {
    let mut dst = vec![];
    read_dir_recursive_impl(&mut dst, path.as_ref());
    dst
}

fn read_dir_recursive_impl(dst: &mut Vec<PathBuf>, path: &Path) {
    let push_file = |dst: &mut Vec<PathBuf>, path: PathBuf| {
        // skip _xxx.slt file
        if regex::Regex::new(r"/_.*\.slt")
            .unwrap()
            .is_match(path.to_str().unwrap())
        {
            println!("skip file: {:?}", path);
        } else {
            dst.push(path);
        }
    };

    if path.is_dir() {
        let entries = std::fs::read_dir(path).unwrap();
        for entry in entries {
            let path = entry.unwrap().path();

            if path.is_dir() {
                read_dir_recursive_impl(dst, &path);
            } else {
                push_file(dst, path);
            }
        }
    } else {
        push_file(dst, path.to_path_buf());
    }
}
