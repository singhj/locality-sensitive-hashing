def all_matching_files(zip_reader, filename, pattern):
    with zip_reader.open(filename) as file_reader:
        (lno, mno) = (0, 0,)
        for line in file_reader:
            found_pattern = pattern.search(line)
            lno += 1
            if found_pattern:
                mno += 1
                yield lno, mno, found_pattern.group(1), found_pattern.group(2)
