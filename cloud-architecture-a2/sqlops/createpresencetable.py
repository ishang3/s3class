import psycopg2


class Partition:
    def __init__(self,tablename,shiftname):
        self.connection = psycopg2.connect(
            host='sensable-2.cesrxorctwi1.us-east-2.rds.amazonaws.com',
            port=5432,
            user='postgres',
            password='sensable01',
            database='demo'
        )
        self.triple = '"""' #this includes the 3 double quotes we need """"
        self.table_name = tablename
        self.shift_name = shiftname
        self.cursor = self.connection.cursor()
        self.partitions_list = None
        self.triggerfn_list = None
        self.enumerate_time()
        self.create_table()
       
        self.create_partitions()
        self.create_trigger_fn()
        self.create_trigger()
        self.connection.commit()

    def enumerate_time(self,):
        count = 0 
        partitions_list = []
        triggerfn_list = []
        for day in range(7): #this value should be 7
            for hour in range(24): #this value should be 24
                for minute in range(0,60,10): #this value should be 0,60,10
                    lower, upper = self.create_time_bounds(hour,minute)
                    #this creates the partition table name 
                    part_table_name = self.create_partition_table_name(count,self.table_name)
                    ret = self.create_partition_table(day,lower,upper,part_table_name,self.table_name)
                    ret2 = self.create_trigger_fn_table(day,lower,upper,part_table_name,count)
                    partitions_list.append(ret)
                    triggerfn_list.append(ret2)
                    count += 1
    
    
        #convert partitions list to a tuple
        partitions_list = tuple(partitions_list)
        self.partitions_list = partitions_list
    
    
        triggerfn_full = ''
        for val in triggerfn_list:
            triggerfn_full += val
        
        self.triggerfn_list = triggerfn_full
       
    
    def create_trigger(self,):

        trigger = (
        """
        CREATE TRIGGER insert_vendors_trigger
           BEFORE INSERT ON vendors
           FOR EACH ROW EXECUTE PROCEDURE vendors_insert_trigger();
        """ ) 

        return trigger
    
    def create_partition_table(self,day,lower,upper,table_name,inherited):

        a_str = """
       
        CREATE TABLE {tablename} (
           CHECK (day  = {day} AND timeshort>='{time_lower_bound}' AND timeshort<'{time_upper_bound}' )
            ) INHERITS ({inherited});

       
       """.format(str1=f"{self.triple}",
                  tablename=f"{table_name}",
                  day=f"{day}",
                  time_lower_bound=f"{lower}",
                  time_upper_bound=f"{upper}",
                  inherited = f"{inherited}"
                  )
        return a_str

    def create_trigger_fn_table(self,day,lower,upper,table_name,count):

        if count == 0:
            log = 'IF'
        else:
            log = 'ELSEIF'

        a_str = """
        {logic}  (NEW.day  = {day} AND NEW.timeshort>='{lower}' AND NEW.timeshort<'{upper}' )
             THEN INSERT INTO  {tablename} VALUES (NEW.*);
       
       """.format(str1=f"{self.triple}",
                  tablename=f"{table_name}",
                  day=f"{day}",
                  lower=f"{lower}",
                  upper=f"{upper}",
                  logic=f"{log}"
                  )
    
        return a_str

    def create_time_bounds(self,hour,minute):

        """
        This creates bounds of the timing intervals
        """

        lower = f'{hour}:{minute}:00'
        
        if minute + 10 == 60:
            higher = f'{hour+1}:{00}:00'
        else:
            higher = f'{hour}:{minute+10}:00'

        return lower,higher

    def create_partition_table_name(self,count,prename):

        """
        takes in count and the prename of the table
        """
        return f'{prename}_{count}'
    


    
    def create_table(self):

        
        #this creates the raw presence table
        commands = (

            """
            CREATE TABLE {tablename} (
            uniquerowid SERIAL,
            day int NOT NULL,
            trackid int,
            type VARCHAR NOT NULL,
            timeshort time NOT NULL,
            starting TIMESTAMP NOT NULL,
            ending TIMESTAMP NOT NULL,
            objectvalue VARCHAR
            ); 
            """.format(tablename = self.table_name)
        )

        #this will create the shift name table
        commands_2 = (

            """
            CREATE TABLE {tablename} (
            uniquerowid SERIAL,
            type VARCHAR NOT NULL,
            objectvalue VARCHAR NOT NULL,
            count int NOT NULL,
            load time NOT NULL,
            starting TIMESTAMP NOT NULL
            ); 
            """.format(tablename = self.shift_name)
        )

        try:
            self.cursor.execute(commands)
            self.cursor.execute(commands_2)
            #self.connection.commit()
        except:
            print('table was not created')

    
    def create_partitions(self):
        """
        This iterates through the created partitons in enumerate_time
        """
        for partition in self.partitions_list:
            print(partition)
            print('**********')
            self.cursor.execute(partition)
        
    
    def create_trigger(self):

        trigger = (
        """
        CREATE TRIGGER insert_{tablename}_trigger
           BEFORE INSERT ON {tablename}
           FOR EACH ROW EXECUTE PROCEDURE {tablename}_insert_trigger();
        """.format(tablename = self.table_name) )

        self.cursor.execute(trigger) 


    def create_trigger_fn(self):



        triggerfn = (
        """
        CREATE OR REPLACE FUNCTION {tablename}_insert_trigger()
        RETURNS TRIGGER AS $$
        BEGIN
           {triggerfn}
           ELSE
               RAISE EXCEPTION 'Date out of range.  Fix the measurement_insert_trigger() function!';
           END IF;
           RETURN NULL;
        END;
        $$
        LANGUAGE plpgsql;
        """.format(triggerfn = self.triggerfn_list,
                   tablename = self.table_name)
        
        )

        self.cursor.execute(triggerfn)

        
            


#p = Partition(tablename='ycompany_1_presence')