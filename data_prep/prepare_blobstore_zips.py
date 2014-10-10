import sys, re, zipfile, uuid

url_file_pattern = re.compile('^."id":"([^"]*)","url":"([^"]*)".*')
text_file_pattern = re.compile('^{"id":"([^"]*):html","text":"(.*)}', flags=re.DOTALL)

def all_matching_files(zip_reader, filename, pattern):
    with zip_reader.open(filename) as file_reader:
        (lno, mno) = (0, 0,)
        for line in file_reader:
            found_pattern = pattern.search(line)
            lno += 1
            if found_pattern:
                mno += 1
                yield lno, mno, found_pattern.group(1), found_pattern.group(2)

def url_line(_id, text):
    return '{"id":"%s","url":"%s"}' % (_id, text)

def text_line(_id, text):
    return '{"id":"%s:html","text":"%s"}' % (_id, text)

def write_the_file(zip_out, filename, texts):
    if len(texts):
        zip_out.writestr(filename, 
                         '\n'.join(texts), 
                         compress_type = zipfile.ZIP_DEFLATED)

if __name__ == "__main__":
    zipfile_names = sys.argv[1:]
    for zipfile_name in zipfile_names:
        with zipfile.ZipFile(zipfile_name, 'r') as zip_in:
            out_file_name = zipfile_name.replace('zip', 'chunked.zip')
#             shutil.copyfile(zipfile_name, out_file_name)
            with zipfile.ZipFile(out_file_name, 'w') as zip_out:
                texts = []
                _ids = []
                for lno, mno, _id, text in all_matching_files(zip_in, 'text.out', text_file_pattern):
                    _ids.append(_id)
                    texts.append(text_line(_id,text))
                    if (len(texts) % 40) == 0:
                        write_the_file(zip_out, str(uuid.uuid4())+'.out', texts)
                        texts = []
                write_the_file(zip_out, str(uuid.uuid4())+'.out', texts)
                urls = []
                for lno, mno, _id, text in all_matching_files(zip_in, 'url.out', url_file_pattern):
                    if _id in _ids:
                        urls.append(url_line(_id,text))
                # skip writing url.out for now
                # write_the_file(zip_out, 'url.out', urls)
                    
