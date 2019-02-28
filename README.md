# mongoio - The Command-Line MongoDB Import/Export Tool
Possibly the most comfortable MongoDB Import/Export tools written on Python.

这可能是最好的使用Python进行MongoDB导入/导出的小工具了

# Feature
* Enough simple
* Support import from a json array file
* Support export data to a json array file
* Realtime ETA
* 

# Download
````wget https://raw.githubusercontent.com/DGideas/mongoio/master/mongoio.py````

# Usage
````shell
./mongoio.py [import|export|input|output|dump] <flags> filename[.json]

        -h, --host      Default: 127.0.0.1
        -p, --port      Default: 27017
        -d, --db        Default: test
        -c, --coll, --collection        Default: test
        -Y, --noconfirm Default: False
        -H, --help      Show this help message
````

# Example
````bash
./mongoio.py dump --db=company --coll=production daily_backup.json
./mongoio.py import something_cool.json --db=universe --coll=earth
````

# Depend
It depends on ```pymongo```, just use ```pip3 install pymongo``` install it!

# Author
* 王万霖 <dgideas@outlook.com>
