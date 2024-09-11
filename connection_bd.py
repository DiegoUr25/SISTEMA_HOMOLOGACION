import pypyodbc as odbc 

DRIVER_NAME = 'SQL Server' 
SERVER_NAME = 'FREYA' 
DATABASE_NAME = 'DB_CARGA_D' 
 
connection_string = 'DRIVER={'+DRIVER_NAME+'};SERVER='+SERVER_NAME+';DATABASE='+DATABASE_NAME+';Trusted_Connection=yes'
# cnxn = odbc.connect('DRIVER={SQL Server};SERVER=FREYA;DATABASE=DB_CARGA_D;Trusted_Connection=yes')
print(connection_string)
conn = odbc.connect(connection_string) 
print(conn)