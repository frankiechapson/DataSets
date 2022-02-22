
# Data sets

## Very easy and simple but useful tool to manage query results as normal data sets in memory

Sometimes, especially from APEX, I need to use a table such a matrix data set.
That's why I created this package.
It can manage many data sets, identified by its name and definied by a SELECT command.
But all data value in these data sets are stored as varchar2!

Here are the list of procedures and functions of the package:

Create a new data set named "ETL" and it is the copy of the ETL_TABLES table:

    begin
        PKG_DS.CREATE_DS( 'ETL', 'select * from ETL_TABLES' );
    end;

The number of rows of the "ETL" data set:
    
    select PKG_DS.NOF_ROWS     ( 'ETL' ) from dual;

The number of columns of the "ETL" data set:

    select PKG_DS.NOF_COLS     ( 'ETL' ) from dual;  

The name of the first column of the "ETL" data set:

    select PKG_DS.GET_COL_NAME ( 'ETL', 1 ) from dual;

The ID of the column of the "ETL" data set what name is "LOCAL_NAME":

    select PKG_DS.GET_COL_ID   ( 'ETL', 'LOCAL_NAME' ) from dual;

The value of 3rd row and 2nd column of the "ETL" data set:

    select PKG_DS.GET_VALUE    ( 'ETL', 3, 2 ) from dual;

The list of column values from the 3rd row of the "ETL" data set:

    select * from table( PKG_DS.GET_ROW ( 'ETL', 3 ) );

The list of row values from the 3rd column of the "ETL" data set:

    select * from table( PKG_DS.GET_COL ( 'ETL', 3 ) );

The list of column names of the "ETL" data set:

    select * from table( PKG_DS.GET_ALL_COL_NAMES ( 'ETL' ) );

And finally drop the data set and free the space:

    begin
        PKG_DS.DROP_DS( 'ETL' );
    end;
   

