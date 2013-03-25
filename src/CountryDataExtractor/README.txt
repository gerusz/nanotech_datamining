Usage:

After starting, the program runs in an interactive console.
Questions:

* host:port, user, password, database name: trivial.

* Source table name: table that contains the source data, for example, tbl_articles

* Destination table name: table that will contain the ID-separated data pairs, for example, article_country

* Column to separate: a column in the source table that contains multiple values, for example, countries

* Separator string: a string that separates the values in that column, for example, ;

* Unique ID column name in the source table: pretty trivial, for example, id.

* Unique ID column name in the destination table: this will store the values from the column above as a foreign key. For example: fk_article_id.

* Separated column name in the destination table: trivial, for example: country

Once you entered these, the program will try to create the table. If it can't, it will print out the SQL commands you need to run to create the destination table.

The program will use the temporary file C:\tmp.csv!