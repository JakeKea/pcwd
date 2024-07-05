from os import listdir, path, rename


#Return a list of files in the specifed directory with the specified extension
def fetch_new_files(dir, ext=".xlsx"):

    #List of files in the new_data directory
    new_data_files = listdir(dir)

    #Store all valid data files in an array
    scanned_files = []
        
    #Check files in the directory are of the correct file type
    for ndf in new_data_files:
        if ndf.endswith(ext):
            scanned_files.append(ndf)

    return scanned_files

#Archive the data file
def archive_data_file(filename, dir, ext=".xlsx"):
    #Rename the current file to archive it
    old_filename = f"{dir}/{filename}"
    new_filename = f"{dir}/archive/{filename}"
    rename(old_filename, new_filename)