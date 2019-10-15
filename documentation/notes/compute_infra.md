# Setup research instance
## Vendor




## Folders structure

### - wd

Folder with users home directories.
Usually we use separate volume for this folder.
Using this approach we can guaranty that if user in some accident reason 
fill all available space in **wd** it won't affect the system. 

### - data

Folder where we keep all generated data if this data should be kept on local drive.
Usually we use separate volume for this folder.
Think bout this way of storing data as an intermediate storage. Preferable way to store the data is **S3**.



### - dump_data

Folder where we keep data that were dumped during some process. 
For example:  

### - http


### Home directories

### Data directories
