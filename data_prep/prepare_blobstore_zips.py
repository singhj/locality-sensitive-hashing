import sys, re, zipfile, shutil

text_file_pattern = re.compile('^{"id":"([^"]*):html","text":"(.*)}', flags=re.DOTALL)
def all_matching_files(zip_reader, filename, pattern):
        with zip_reader.open(filename) as file_reader:
            lno = 0
            for line in file_reader:
                found_pattern = pattern.search(line)
                if found_pattern:
                    # we only count the lines that match. this is deliberate
                    lno += 1
                    _id = found_pattern.group(1)
                    text = found_pattern.group(2)
                    text.replace('\\n',' ')
                    yield lno, _id, text


if __name__ == "__main__":
    zipfile_names = sys.argv[1:]
    for zipfile_name in zipfile_names:
        with zipfile.ZipFile(zipfile_name, 'r') as input_zip:
            out_file_name = zipfile_name.replace('zip', 'blobstore.zip')
            shutil.copyfile(zipfile_name, out_file_name)
            with zipfile.ZipFile(out_file_name, 'a') as zip_writer:
                for lno, _id, text in all_matching_files(input_zip, 'text.out', text_file_pattern):
                    zip_writer.writestr(_id+'txt', text, compress_type = zipfile.ZIP_DEFLATED)
