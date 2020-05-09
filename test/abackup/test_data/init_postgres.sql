
CREATE USER foouser WITH PASSWORD 'changethisterrriblepassword';

CREATE DATABASE bardb
 ENCODING 'UTF8'
 LC_COLLATE='C'
 LC_CTYPE='C'
 template=template0
 OWNER foouser;
