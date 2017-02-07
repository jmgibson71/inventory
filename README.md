# inventory
Contains utility programs for the SANC Digital Services Section.  Designed to "quickly" index a drive and then search for duplicates either
on itself or files in another location.

Requires MySQL at the moment.

Also requires a config file in the running directory `db_config.cfg`

This config file should be structured like this replace the <> with your
own values to connect to your instance of a mysql database:

[DATABASE]
USER=\<USER\>
PASS=\<PASSWORD\>
HOST=127.0.0.1
DATABASE=mysql

