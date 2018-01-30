import os, sys
import hashlib
import shutil
import argparse

def findDup(parentFolder, all_files):
    # Dups in format {hash:[names]}
    dups = {}
    for dirName, subdirs, fileList in os.walk(parentFolder):
        print('Scanning %s...' % dirName)
        for filename in fileList:
            # Get the path to the file
            path = os.path.join(dirName, filename)
            all_files.append(path)
            # Calculate hash
            file_hash = hashfile(path)
            # Add or append the file path
            if file_hash in dups:
                dups[file_hash].append(path)
            else:
                dups[file_hash] = [path]
    return dups

# Joins two dictionaries
def joinDicts(dict1, dict2):
    for key in dict2.keys():
        if key in dict1:
            dict1[key] = dict1[key] + dict2[key]
        else:
            dict1[key] = dict2[key]

def hashfile(path, blocksize = 65536):
    afile = open(path, 'rb')
    hasher = hashlib.md5()
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    afile.close()
    return hasher.hexdigest()

def processResults(dict1, all_files, destinationFolder):
    dupes = []
    results = list(filter(lambda x: len(x) > 1, dict1.values()))
    if len(results) is 0:
        print('No duplicate files found.')
        return;    
    
    print('Duplicates Found:')
    print('___________________')
    for result in results:
        result.pop(0)
        for subresult in result:
            print('\t%s' % subresult)            
            dupes.append(subresult)
        
 
    unique_set = set(all_files) - set(dupes)
    
    print("Unique Files:")
    print('___________________')
    for file in unique_set:
        print('\t%s' % file)            
        safe_copy(file, destinationFolder)
            

def safe_copy(file_path, out_dir):
    """Safely copy a file to the specified directory. If a file with the same name already 
    exists, the copied file name is altered to preserve both.

    :param str file_path: Path to the file to copy.
    :param str out_dir: Directory to copy the file into.
    """

    name = os.path.basename(file_path).lower()
    if not os.path.exists(os.path.join(out_dir, name)):
        shutil.copy(file_path, os.path.join(out_dir, name))
    else:
        base, extension = os.path.splitext(name)
        i = 1
        while os.path.exists(os.path.join(out_dir, '{}_{}{}'.format(base, i, extension))):
            i += 1
        shutil.copy(file_path, os.path.join(out_dir, '{}_{}{}'.format(base, i, extension)))


if __name__ == '__main__':
    desc='Find unique files in a folder.'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    parser.add_argument("--data_folder", help="a folder full of files/folders of other files")
    parser.add_argument("--output_folder", help="a folder to place copies of unique files")
    
    args = parser.parse_args()
    print("Index selected: " + str(args.data_folder))
    print("Outfile selected: " + str(args.output_folder))
    
    if not args.data_folder or not args.output_folder:
        print('Usage: python3 dedupe.py --data_folder <data folder> --output_folder <existing destination folder>')
        exit()
        
    dups = {}
    all_files = []
    folder = args.data_folder
    destinationFolder= args.output_folder
    # Iterate the folders given
    if os.path.exists(folder):
        # Find the duplicated files and append them to the dups
        joinDicts(dups, findDup(folder, all_files))
    else:
        print('%s is not a valid path, please verify' % folder)
        sys.exit()
    processResults(dups, all_files, destinationFolder)




