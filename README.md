Version 2 of the crawler for the Decentralized Search project https://rorur.com


Build each of the top level files and run them as systemd daemons ( see example.service file). 

Create file /etc/dse/dse.conf with the following information:

directory where data structures will be located

available disk space in GB ( at the moment, we do only namespace crawl so this number must be amortized: multiply your actual disk space by 10^4)

cpu usage ( target cpu usage for single processor)

bandwidth in MB/s ( target BW usage)

