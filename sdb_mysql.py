import MySQLdb

class mysql:
   """mysql is an interface to the sql library and specifically simplifies 
      the steps of returning objects from a call to a mysql database

      Parameters
      ----------
      host: string
           host name of mysql database
      dbname: string
           name of mysql database
      user: string
           user for database
      passwd: string
           password of user for mysql database

   """
   
   def __init__(self, host,dbname,user,passwd, port=None):
        self.db = MySQLdb.connect(host=host,db=dbname,user=user,passwd=passwd, port=port)

   @classmethod
   def fromuri(cls, uri):
       result = uri.split('/')
       dbname=result[3]
       result = result[2].split('@')
       host = result[1]
       result = result[0].split(':')
       user = result[0]
       passwd=result[1]
       return cls(host, dbname, user=user, passwd=passwd)
       

   def select(self, selection, table, logic):
       """Select a record from a table

       Parameters
       ----------
       selection: string
           columns to return 
       table: string
           table or group of tables to select from
       logic: string
           logic for selecting from table

       Returns
        -------
       record: list
           list of results 

           
       """

       #build the command
       record=''
       exec_command    =""
       exec_command   +="SELECT "+selection
       exec_command   +=" FROM  "+table
       if len(logic)>0:
           exec_command   +=" WHERE  "+logic

       #execute the command
       cursor = self.db.cursor()
       cursor.execute(exec_command)
       record = cursor.fetchall()
       cursor.close()

       #clean the return record
       if len(record)>0:
           return record
       else:
           return record

       return record


   def update(self, insertion, table, logic):
       """Select a record from a table

       Parameters
       ----------
       insertion: string
           values and columns to insert
       table: string
           table or group of tables to select from
       logic: string
           logic for selecting from table

       """

       #build the command
       record=''
       exec_command    =""
       exec_command   +="UPDATE "+table
       exec_command   +=" SET  "+insertion
       if len(logic)>0:
           exec_command   +=" WHERE  "+logic

       #execute the command
       cursor = self.db.cursor()
       cursor.execute(exec_command)
       cursor.execute("COMMIT")


   def insert(self, insertion, table):
       """Select a record from a table

       Parameters
       ----------
       insertion: string
           values and columns to insert
       table: string
           table or group of tables to select from

           
       """

       #build the command
       exec_command    =""
       exec_command   +="INSERT INTO "+table
       exec_command   +=" SET  "+insertion
 
    
       #execute the command
       cursor = self.db.cursor()
       try:
           cursor.execute(exec_command)
           cursor.execute("COMMIT")
       except MySQLdb.IntegrityError,e:
           if str(e).count('Duplicate entry'): return
           raise MySQLdb.IntegrityError(e)
           
       except Exception,e:
           raise Exception(str(e) + exec_command)


