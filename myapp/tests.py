from django.test import TestCase, Client

# Create your tests here.
class ServerSelectTest(TestCase):
    c = Client()
    c2 = Client()
    c3 = Client()
    c4 = Client()
    c5 = Client()

    def testGetServer(self):
        response = self.c.get('')
        self.assertEqual(response.status_code,302)
        pass

    def testGetServerForm(self):
        response = self.c.get('/connectserver/')
        self.assertEqual(response.status_code,200)
    
    def testPostServerFormMysqlSuccess(self):
        response = self.c.post('/connectserver/',{'database_type':'mysql', 'username':'root', 'password':'root', 'host':'localhost', 'port':3306})
        self.assertEqual(response.status_code,302)
    
    def testPostServerFormMysqlFail(self):
        response = self.c.post('/connectserver/',{'database_type':'mysql', 'username':'root1', 'password':'root', 'host':'localhost', 'port':3306})
        self.assertEqual(response.status_code,302)
    
    def testPostServerFormPostgresSuccess(self):
        response = self.c.post('/connectserver/',{'database_type':'postgres', 'username':'postgres', 'password':'root', 'host':'localhost', 'port':5432, 'database':'postgres'})
        self.assertEqual(response.status_code,302)
    
    def testPostServerFormPostgresFail(self):
        response = self.c.post('/connectserver/',{'database_type':'postgres', 'username':'postgres1', 'password':'root', 'host':'localhost', 'port':5432, 'database':'postgres'})
        self.assertEqual(response.status_code,302)
    
    def testGetDatabaseList(self):
        self.c.post('/connectserver/',{'database_type':'mysql', 'username':'root', 'password':'root', 'host':'localhost', 'port':3306})
        response = self.c.get('/listdatabase/')
        self.assertEqual(response.status_code,200)
    
    def testPostDatabaseListMysql(self):
        self.c.post('/connectserver/',{'database_type':'mysql', 'username':'root', 'password':'root', 'host':'localhost', 'port':3306})
        with open('data_file.csv') as csvfile:
            response = self.c.post('/listdatabase/',{'database':'rental', 'table':'rental_vehicletype','csvfile':csvfile})
        self.assertEqual(response.status_code,200)
    
    def testPostDatabaseListPostgres(self):
        self.c.post('/connectserver/',{'database_type':'postgres', 'username':'postgres', 'password':'root', 'host':'localhost', 'port':5432, 'database':'postgres'})
        with open('data_file.csv') as csvfile:
            response = self.c.post('/listdatabase/',{'database':'postgres', 'table':'mytable','csvfile':csvfile})
        
        pass
    
    def testMultipleUserAccess(self):
        self.c.post('/connectserver/',{'database_type':'mysql', 'username':'root', 'password':'root', 'host':'localhost', 'port':3306})
        self.c2.post('/connectserver/',{'database_type':'postgres', 'username':'postgres', 'password':'root', 'host':'localhost', 'port':5432, 'database':'postgres'})
        self.c3.post('/connectserver/',{'database_type':'mysql', 'username':'root', 'password':'root', 'host':'localhost', 'port':3306})
        self.c4.post('/connectserver/',{'database_type':'mysql', 'username':'root', 'password':'root', 'host':'localhost', 'port':3306})
        self.c5.post('/connectserver/',{'database_type':'mysql', 'username':'root', 'password':'root', 'host':'localhost', 'port':3306})

        with open('data_file.csv') as csvfile:
            response1 = self.c.post('/listdatabase/',{'database':'rental', 'table':'rental_vehicletype','csvfile':csvfile})
        with open('data_file.csv') as csvfile:
            response2 = self.c2.post('/listdatabase/',{'database':'postgres', 'table':'mytable','csvfile':csvfile})
        with open('data_file.csv') as csvfile:
            response3 = self.c3.post('/listdatabase/',{'database':'rental', 'table':'rental_vehicletype','csvfile':csvfile})
        with open('data_file.csv') as csvfile:
            response4 = self.c4.post('/listdatabase/',{'database':'rental', 'table':'rental_vehicletype','csvfile':csvfile})
        with open('data_file.csv') as csvfile:
            response5 = self.c5.post('/listdatabase/',{'database':'rental', 'table':'rental_vehicletype','csvfile':csvfile})
        
        self.assertEqual(response1.status_code,200)
        self.assertEqual(response2.status_code,200)
        self.assertEqual(response3.status_code,200)
        self.assertEqual(response4.status_code,200)
        self.assertEqual(response5.status_code,200)
