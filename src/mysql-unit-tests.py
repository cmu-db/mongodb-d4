#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import sql2mongo

class TestConversions (unittest.TestCase) :
    
    def setUp(self) :
        self.schema = {'users': ['a', 'b']}
        self.mongo = sql2mongo.Sql2mongo(self.schema)
    
    def test_delete01(self) :
        sql = 'DELETE FROM users WHERE z="abc"'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.remove({z:'abc'})", result[0])
        
    def test_insert01(self) :
        sql = 'INSERT INTO users VALUES (3,5)'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u'db.users.insert({a:3,b:5})', result[0])
        
    def test_queryTypeSelect(self) :
        sql = 'SELECT a,b FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.query_type
        self.assertEqual('SELECT', result)
        
    def test_select01(self) :
        sql = 'SELECT * FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u'db.users.find()', result[0])
    
    def test_select02(self) :
        sql = 'SELECT a,b FROM users'
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.find({}, {a:1,b:1})", result[0])

    def test_update01(self) :
        sql = "UPDATE users SET a=1 WHERE b='q'"
        self.mongo.process_sql(sql)
        result = self.mongo.render_mongo_command()
        self.assertEqual(u"db.users.update({b:'q'}, {$set:{a:1}}, false, true)", result[0])
## END CLASS

if __name__ == '__main__':
    unittest.main()
## END MAIN