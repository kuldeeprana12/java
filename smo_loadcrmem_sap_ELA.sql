set serveroutput on;
set heading off;
select 'Start Time : ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') from dual;
begin
	ol00.pkg_load_crmem_sap_ELA.prc_load_crmem_sap;
exception
	when others then
    dbms_output.put_line('Sql Cd: '||to_char(sqlcode));
    dbms_output.put_line('Sql Msg: '||substr(sqlerrm,1,200));
    dbms_output.put_line('Ret Msg: ');
end;
/
exit;
