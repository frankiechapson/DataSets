/************************************************************
    Author  :   TothF  
    Remark  :   Data Sets from queries
    Date    :   2022.02.22
************************************************************/


create or replace package PKG_DS as

/********************************************************************************************************************

    This package can manage data set in memory.
    But all values are stored as varchar2!!!
        
    History of changes
    yyyy.mm.dd | Version | Author         | Changes
    -----------+---------+----------------+-------------------------
    2022.02.22 |  1.0    | Ferenc Toth    | Created 

********************************************************************************************************************/


    ------------------------------------------------------------------
    procedure CREATE_DS    ( I_DS_NAME  in varchar2
                           , I_SQL      in varchar2 := 'select * from dual' 
                           );
    ------------------------------------------------------------------
    procedure DROP_DS      ( I_DS_NAME  in varchar2 );
    ------------------------------------------------------------------
    function  NOF_ROWS     ( I_DS_NAME  in varchar2 ) return number;
    ------------------------------------------------------------------
    function  NOF_COLS     ( I_DS_NAME  in varchar2 ) return number;
    ------------------------------------------------------------------
    function  GET_COL_NAME ( I_DS_NAME  in varchar2 
                           , I_COL_ID   in number 
                           ) return varchar2;
    ------------------------------------------------------------------
    function  GET_COL_ID   ( I_DS_NAME  in varchar2 
                           , I_COL_NAME in varchar2 
                           ) return number;
    ------------------------------------------------------------------
    function  GET_VALUE    ( I_DS_NAME  in varchar2 
                           , I_ROW_ID   in number
                           , I_COL_ID   in number 
                           ) return varchar2;
    ------------------------------------------------------------------
    function  GET_ROW      ( I_DS_NAME  in varchar2 
                           , I_ROW_ID   in number
                           ) return T_STRING_LIST PIPELINED;
    ------------------------------------------------------------------
    function  GET_COL      ( I_DS_NAME  in varchar2 
                           , I_COL_ID   in number
                           ) return T_STRING_LIST PIPELINED;
    ------------------------------------------------------------------
    function GET_ALL_COL_NAMES  ( I_DS_NAME  in varchar2  ) return T_STRING_LIST PIPELINED;
    ------------------------------------------------------------------

end;
/




create or replace package body PKG_DS as

/********************************************************************************************************************

    This package can manage data set in memory.
    
    How to use? From SQL or PL/SQL (example):

    begin
        PKG_DS.CREATE_DS( 'ETL', 'select * from ETL_TABLES' );
    end;
    
    select PKG_DS.NOF_ROWS     ( 'ETL' ) from dual;
    select PKG_DS.NOF_COLS     ( 'ETL' ) from dual;  
    select PKG_DS.GET_COL_NAME ( 'ETL', 1 ) from dual;
    select PKG_DS.GET_COL_ID   ( 'ETL', 'LOCAL_NAME' ) from dual;
    select PKG_DS.GET_VALUE    ( 'ETL', 3, 2 ) from dual;
    select * from table( PKG_DS.GET_ROW ( 'ETL', 3 ) );
    select * from table( PKG_DS.GET_COL ( 'ETL', 3 ) );
    select * from table( PKG_DS.GET_ALL_COL_NAMES ( 'ETL' ) );

    begin
        PKG_DS.DROP_DS( 'ETL' );
    end;
   
        
    History of changes
    yyyy.mm.dd | Version | Author         | Changes
    -----------+---------+----------------+-------------------------
    2022.02.22 |  1.0    | Ferenc Toth    | Created 

********************************************************************************************************************/

    ------------------------------------------------------------------

    type T_COLS          is table of varchar2(4000);
    type T_ROWS          is table of T_COLS;
    type T_COL_IDS       is table of number    index by varchar2(4000);

    type T_DSS_COL_NAMES is table of T_COLS    index by varchar2(4000);
    type T_DSS_COL_IDS   is table of T_COL_IDS index by varchar2(4000);
    type T_DSS_VALUES    is table of T_ROWS    index by varchar2(4000);

    ------------------------------------------------------------------

    G_DSS_COL_NAMES     T_DSS_COL_NAMES ;
    G_DSS_COL_IDS       T_DSS_COL_IDS   ;
    G_DSS_VALUES        T_DSS_VALUES    ;

    ------------------------------------------------------------------
  

    ------------------------------------------------------------------
    procedure CREATE_DS    ( I_DS_NAME  in varchar2
                           , I_SQL      in varchar2 := 'select * from dual' 
                           ) as
    ------------------------------------------------------------------
        V_DATA              sys_refcursor;
        V_CURSOR            integer;
        V_COLUMNS           integer;
        V_DESC              dbms_sql.desc_tab;
        V_STR               varchar2( 4000 );
    begin
        DROP_DS( I_DS_NAME );

        open V_DATA for I_SQL;

        V_CURSOR := dbms_sql.to_cursor_number( V_DATA );

        dbms_sql.describe_columns( V_CURSOR, V_COLUMNS, V_DESC );

        G_DSS_COL_NAMES( I_DS_NAME ) := new T_COLS();

        for V_I in 1..V_COLUMNS 
        loop

            G_DSS_COL_IDS( I_DS_NAME)( V_DESC( V_I ).col_name ) := V_I;

            G_DSS_COL_NAMES( I_DS_NAME ).extend;
            G_DSS_COL_NAMES( I_DS_NAME )( G_DSS_COL_NAMES( I_DS_NAME ).count ) := V_DESC( V_I ).col_name;

        end loop;

        for V_I in 1..V_COLUMNS 
        loop
            dbms_sql.define_column( V_CURSOR, V_I, V_STR, 4000 );
        end loop;

        G_DSS_VALUES( I_DS_NAME ) := new T_ROWS();

        while dbms_sql.fetch_rows( V_CURSOR ) > 0 
        loop
            G_DSS_VALUES( I_DS_NAME ).extend;
            G_DSS_VALUES( I_DS_NAME )( G_DSS_VALUES( I_DS_NAME).count ) := new T_COLS();
            for V_I in 1..V_COLUMNS 
            loop
                dbms_sql.column_value( V_CURSOR, V_I, V_STR );
                G_DSS_VALUES( I_DS_NAME)( G_DSS_VALUES( I_DS_NAME ).count ).extend;
                G_DSS_VALUES( I_DS_NAME)( G_DSS_VALUES( I_DS_NAME ).count )( V_I ) := V_STR;
            end loop;
        end loop;

        dbms_sql.close_cursor( V_CURSOR );

    end;


    ------------------------------------------------------------------
    procedure DROP_DS      ( I_DS_NAME  in varchar2 ) as
    ------------------------------------------------------------------
    begin
        G_DSS_COL_NAMES ( I_DS_NAME ).delete;
        G_DSS_COL_IDS   ( I_DS_NAME ).delete;
        G_DSS_VALUES    ( I_DS_NAME ).delete;
    exception when others then
        null;
    end;


    ------------------------------------------------------------------
    function  NOF_ROWS     ( I_DS_NAME  in varchar2 ) return number as
    ------------------------------------------------------------------
    begin
        return G_DSS_VALUES( I_DS_NAME ).count;
    exception when others then
        return null;
    end;
    
    
    ------------------------------------------------------------------
    function  NOF_COLS     ( I_DS_NAME  in varchar2 ) return number as
    ------------------------------------------------------------------
    begin
        return G_DSS_COL_NAMES( I_DS_NAME ).count;
    exception when others then
        return null;
    end;


    ------------------------------------------------------------------
    function  GET_COL_NAME ( I_DS_NAME  in varchar2 
                           , I_COL_ID   in number 
                           ) return varchar2 as 
    ------------------------------------------------------------------
    begin
        return G_DSS_COL_NAMES( I_DS_NAME)( I_COL_ID );
    exception when others then
        return null;
    end;


    ------------------------------------------------------------------
    function  GET_COL_ID   ( I_DS_NAME  in varchar2 
                           , I_COL_NAME in varchar2 
                           ) return number as 
    ------------------------------------------------------------------
    begin
        return G_DSS_COL_IDS( I_DS_NAME)( I_COL_NAME );
    exception when others then
        return null;
    end;


    ------------------------------------------------------------------
    function  GET_VALUE    ( I_DS_NAME  in varchar2 
                           , I_ROW_ID   in number
                           , I_COL_ID   in number 
                           ) return varchar2 as
    ------------------------------------------------------------------
    begin
        return G_DSS_VALUES( I_DS_NAME)( I_ROW_ID )( I_COL_ID);
    exception when others then
        return null;
    end;


    ------------------------------------------------------------------
    function  GET_ROW      ( I_DS_NAME  in varchar2 
                           , I_ROW_ID   in number
                           ) return T_STRING_LIST PIPELINED as
    ------------------------------------------------------------------
    begin
        for L_I in 1..G_DSS_VALUES( I_DS_NAME)( I_ROW_ID ).count
        loop
            pipe row( G_DSS_VALUES( I_DS_NAME)( I_ROW_ID )( L_I ) );
        end loop;
        return;
    exception when others then
        null;
    end;


    ------------------------------------------------------------------
    function  GET_COL      ( I_DS_NAME  in varchar2 
                           , I_COL_ID   in number
                           ) return T_STRING_LIST PIPELINED as 
    ------------------------------------------------------------------
    begin
        for L_I in 1..G_DSS_VALUES( I_DS_NAME).count
        loop
            pipe row( G_DSS_VALUES( I_DS_NAME)( L_I )( I_COL_ID ) );
        end loop;
        return;
    exception when others then
        null;
    end;


    ------------------------------------------------------------------
    function GET_ALL_COL_NAMES  ( I_DS_NAME  in varchar2  ) return T_STRING_LIST PIPELINED as
    ------------------------------------------------------------------
    begin
        for L_I in 1..G_DSS_COL_NAMES( I_DS_NAME).count
        loop
            pipe row( G_DSS_COL_NAMES( I_DS_NAME)( L_I ) );
        end loop;
        return;
    exception when others then
        null;
    end;


end;
/


