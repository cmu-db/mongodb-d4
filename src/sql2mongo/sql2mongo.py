# -*- coding: utf-8 -*-

import sqlparse

'''
@todo: JOIN processing
@todo: Process basic OR criteria
@todo: Handle nested queries?
'''
class Sql2mongo (object) :
    
    def __init__(self, sql, tbl_cols = {}) : 
        self.sql = sql
        self.stmt = None
        self.query_type = None
        self.tables = {}
        self.select_cols = {}
        self.where_cols = {}
        self.sort_cols = {}
        self.limit = {}
        self.skip = {}
        self.from_loc = None
        self.select_loc = None
        self.insert_loc = None
        self.update_loc = None
        self.delete_loc = None
        self.set_loc = None
        self.join_loc = None
        self.where_loc = None
        self.sort_loc = None
        self.limit_loc = None
        self.skip_loc = None
        self.values_loc = None
        self.errors = [] 
        self.tbl_cols = tbl_cols
        self.process()
    ## ENDDEF
    
    '''
    Helper function to translate the various components parsed from the SQL statement
    to a useable MongoDB command.
    '''
    def compose_mongo(self, db, collection, alias, cmd) :
        mongo = db + '.' + collection + '.' + cmd + '(';
        if len(self.where_cols[alias]) > 0 :
            mongo = mongo + '{' + ','.join(self.where_cols[alias]) + '}';
        ## ENDIF
        if len(self.select_cols[alias]) > 0 :
            if len(self.where_cols[alias]) > 0 :
                mongo = mongo + ', '
            ## ENDIF
            mongo += '{'
            if self.query_type == 'UPDATE' :
                mongo += '$set:{'
            ## ENDIF
            mongo += ','.join(self.select_cols[alias])
            if self.query_type == 'UPDATE' :
                mongo +=  '}'
            ## ENDIF
            mongo += '}'
        ## ENDIF
        if self.query_type == 'UPDATE' :
            mongo += ', false, true'
        ## ENDIF
        mongo = mongo + ')'
        if len(self.sort_cols[alias]) > 0 :
            mongo += '.sort(' + ','.join(self.sort_cols[alias]) + ')'
        ## ENDIF
        if self.limit[alias] <> None :
            mongo += '.limit(' + self.limit[alias] + ')'
        ## ENDIF
        if self.skip[alias] <> None :
            mongo += '.skip(' + self.skip[alias] + ')'
        ## ENDIF
        return mongo
    ## ENDDEF

    '''
    Process a DELETE SQL query
    '''
    def do_delete(self) :
        i = 0
        for token in self.stmt.tokens :
            cls = token.__class__.__name__
            if cls == 'Token' :
                token_uni = token.to_unicode()
                if token_uni == 'DELETE' :
                    self.delete_loc = i
                elif token_uni == 'FROM' :
                    self.delete_loc = i
                ## ENDIF
            elif cls == 'Where' :
                self.where_loc = i
            i += 1
            ## ENDIF
        ## ENDFOR
        
        ''' Determine table name '''
        tbl_token_loc = self.delete_loc + 2
        cls = self.stmt.tokens[tbl_token_loc].__class__.__name__
        if cls == 'Identifier' :
            tbl_name = self.stmt.tokens[tbl_token_loc].to_unicode()
            self.tables['main'] = tbl_name
            self.where_cols['main'] = []
            self.select_cols['main'] = []
            self.sort_cols['main'] = []
            self.limit['main'] = None
            self.skip['main'] = None
        ## ENDIF
        
        ''' PROCESS WHERE clause '''
        if self.where_loc <> None :
            where = self.stmt.tokens[self.where_loc]
            count = len(where.tokens)
            i = 1
            while i < count :
                if where.tokens[i].ttype == sqlparse.tokens.Whitespace :
                    pass 
                else :
                    cls = where.tokens[i].__class__.__name__
                    if cls == 'Token' :
                        pass
                    elif cls == 'Identifier' :
                        offset = 1;
                        if where.tokens[i + 1].ttype == sqlparse.tokens.Whitespace :
                            offset = 2;
                        parts = self.process_where_comparison_separate(where.tokens[i], where.tokens[i+offset], where.tokens[i+offset+offset])
                        self.where_cols[parts[0]].append(parts[1])
                        i += 4
                    elif cls == 'Comparison' :
                        parts = self.process_where_comparison(where.tokens[i])
                        self.where_cols[parts[0]].append(parts[1])
                    ## ENDIF
                ## ENDIF
                i += 1
            ## ENDWHILE
        ## ENDIF
    ## ENDDEF
    
    '''
    Process an INSERT SQL query
    '''
    def do_insert(self) :
        i = 0
        for token in self.stmt.tokens :
            cls = token.__class__.__name__
            if cls == 'Token' :
                token_uni = token.to_unicode()
                if token_uni == 'INSERT' :
                    self.insert_loc = i
                elif token_uni == 'INTO' :
                    self.insert_loc = i
                elif token_uni == 'VALUES' :
                    self.values_loc = i
                ## ENDIF
            ## ENDIF
            i += 1
        ## ENDFOR
        
        ''' Determine table name and columns '''
        tbl_token_loc = self.insert_loc + 2
        cls = self.stmt.tokens[tbl_token_loc].__class__.__name__
        if cls == 'Function' : 
            cols_specified = True
            cols = []
            sub_tokens = self.stmt.tokens[tbl_token_loc].tokens
            tbl_name = sub_tokens[0].to_unicode()
            parens = sub_tokens[2]
            for token in parens.tokens :
                cls = token.__class__.__name__
                if cls == 'IdentifierList' :
                    for tok in token.tokens :
                        tok_cls = tok.__class__.__name__
                        if tok_cls == 'Identifier' :
                            cols.append(tok.to_unicode())
                        ## ENDIF
                    ## ENDFOR
                elif cls == 'Identifier' :
                    cols.append(token.to_unicode())
                elif cls == 'Token' and token.ttype == sqlparse.tokens.Token.Name.Builtin :
                    cols.append(token.to_unicode())
                ## ENDIF
            ## ENDFOR
        elif cls == 'Identifier' :
            tbl_name = self.stmt.tokens[tbl_token_loc].to_unicode()
            cols_specified = False
            cols = self.tbl_cols[tbl_name]
        ## ENDIF
        
        ''' Determine column values '''
        column_values = []
        values_loc = self.values_loc + 2
        for token in self.stmt.tokens[values_loc].tokens :
            cls = token.__class__.__name__
            if cls == 'IdentifierList' :
                for tok in token.tokens :
                    tok_cls = tok.__class__.__name__
                    if tok_cls == 'Identifier' :
                        column_values.append(self.process_where_comparison_value(tok))
                    elif tok.ttype <> sqlparse.tokens.Whitespace and tok.ttype <> sqlparse.tokens.Punctuation :
                        column_values.append(self.process_where_comparison_value(tok))
                    ## ENDIF
                ## ENDFOR
            elif cls == 'Identifier' :
                column_values.append(self.process_where_comparison_value(tok))
            elif cls == 'Token' and token.ttype <> sqlparse.tokens.Punctuation :
                column_values.append(self.process_where_comparison_value(token))
            ## ENDIF
        ## ENDFOR
        print column_values
        self.tables['main'] = tbl_name
        self.select_cols['main'] = []
        self.where_cols['main'] = []
        self.sort_cols['main'] = []
        self.limit['main'] = None
        self.skip['main'] = None
        
        i = 0
        for col in column_values :
            self.where_cols['main'].append(cols[i] + ':' + col)
            i += 1
        ## ENDFOR
    ## ENDDEF
    
    '''
    Process a SELECT SQL query
    '''
    def do_select(self) :
        i = 0
        for token in self.stmt.tokens :
            cls = token.__class__.__name__
            if cls == 'Token' :
                token_uni = token.to_unicode()
                if token_uni == 'SELECT' :
                    self.select_loc = i
                elif token_uni == 'FROM' :
                    self.from_loc = i
                elif token_uni == 'LIMIT' :
                    self.limit_loc = i
                elif token_uni == 'ORDER' :
                    self.sort_loc = i
                elif token_uni == 'BY' :
                    self.sort_loc = i
                elif token_uni == 'SKIP' :
                    self.skip_loc = i
                else :
                    self.errors.append('Unknown Token Type: ' + token_uni)
                ## ENDIF
            elif cls == 'Identifier' :
                if token.to_unicode() == 'SKIP' :
                    self.skip_loc = i
                pass
            elif cls == 'IdentifierList' :
                pass
            elif cls == 'Where' :
                self.where_loc = i
            else :
                self.errors.append('Unknown Token Class: ' + cls)
            ## ENDIF
            i += 1
        ## ENDFOR
        
        ''' Process FROM clause '''
        if self.where_loc <> None :
            end = self.where_loc
        elif self.sort_loc <> None :
            end = self.sort_loc
        elif self.limit_loc <> None :
            end = self.limit_loc
        elif self.skip_loc <> None :
            end = self.skip_loc
        else :
            end = len(self.stmt.tokens)
        ## ENDIF
        for i in range(self.from_loc + 1, end) :
            cls = self.stmt.tokens[i].__class__.__name__
            if cls <> 'Token' :
                parts = self.stmt.tokens[i].to_unicode().split(' ')
                if len(parts) == 1 :
                    index = 'main'
                elif len(parts) == 2 :
                    index = parts[1]
                else :
                    index = parts[2]
                ## ENDIF
                self.tables[index] = parts[0]
                self.select_cols[index] = []
                self.where_cols[index] = []
                self.sort_cols[index] = []
                self.limit[index] = None
                self.skip[index] = None
            ## ENDIF
        ## ENDFOR
        
        ''' Process JOINS '''
        
        ''' Process SELECT clause '''
        for i in range(self.select_loc + 1, self.from_loc) :
            cls = self.stmt.tokens[i].__class__.__name__
            if cls == 'Token' :
                pass
            elif cls == 'IdentifierList' :
                ilist = self.stmt.tokens[i].get_identifiers()
                for il in ilist :
                    parts = il.to_unicode().split('.')
                    if len(parts) == 1 :
                        self.select_cols['main'].append(parts[0] + ':1')
                    else :
                        self.select_cols[parts[0]].append(parts[1] + ':1')
                    ## ENDIF
                ## ENDFOR
            else :
                parts = self.stmt.tokens[i].to_unicode().split('.')
                if len(parts) == 1 :
                    self.select_cols['main'].append(parts[0] + ':1')
                else :
                    self.select_cols[parts[0]].append(parts[1] + ':1')
            ## ENDIF
        ## ENDFOR
        
        ''' PROCESS WHERE clause '''
        if self.where_loc <> None :
            where = self.stmt.tokens[self.where_loc] 
            count = len(where.tokens)
            i = 1
            while i < count :
                if where.tokens[i].ttype == sqlparse.tokens.Whitespace :
                    pass 
                else :
                    cls = where.tokens[i].__class__.__name__
                    if cls == 'Token' :
                        pass
                    elif cls == 'Identifier' :
                        parts = self.process_where_comparison_separate(where.tokens[i], where.tokens[i+2], where.tokens[i+4])
                        self.where_cols[parts[0]].append(parts[1])
                        i += 4
                    elif cls == 'Comparison' :
                        parts = self.process_where_comparison(where.tokens[i])
                        self.where_cols[parts[0]].append(parts[1])
                    ## ENDIF
                ## ENDIF
                i += 1
            ## ENDWHILE
        ## ENDIF
        
        ''' Process ORDER BY '''
        if self.sort_loc <> None :
            if self.limit_loc <> None :
                end = self.limit_loc
            elif self.skip_loc <> None :
                end = self.skip_loc
            else :
                end = len(self.stmt.tokens)
            ## ENDIF
        
            i = self.sort_loc + 1
            while i < end :
                if self.stmt.tokens[i].ttype == sqlparse.tokens.Whitespace :
                    pass
                else :
                    cls = self.stmt.tokens[i].__class__.__name__
                    if cls == 'Identifier' :
                        parts = self.process_identifier(stmt.tokens[i])
                        if self.stmt.tokens[i + 2].ttype == sqlparse.tokens.Token.Punctuation :
                            sort = '1'
                        else :
                            if self.stmt.tokens[i + 2].value == 'ASC' :
                                sort = '1'
                            else :
                                sort = '-1'
                            ## ENDIF
                        self.sort_cols[parts[0]].append('{' + parts[1] + ':' + sort + '}')
                        i += 2
                        ## ENDIF
                    ## ENDIF
                ## ENDIF
                i += 1
            ## ENDWHILE
        ## ENDIF
        
        ''' PROCESS LIMIT '''
        if self.limit_loc <> None :
            for alias, table_name in self.tables.iteritems() :
                self.limit[alias] = self.stmt.tokens[self.limit_loc + 2].to_unicode()
            ## ENDFOR
        ## ENDIF
        
        ''' PROCESS SKIP '''
        if self.skip_loc <> None :
            for alias, table_name in self.tables.iteritems() :
                self.skip[alias] = self.stmt.tokens[self.skip_loc + 2].to_unicode()
            ## ENDFOR
        ## ENDIF
    ## ENDDEF
    
    '''
    Process UPDATE SQL query
    '''
    def do_update(self) :
        i = 0
        print self.stmt.tokens
        for token in self.stmt.tokens :
            cls = token.__class__.__name__
            if cls == 'Token' :
                token_uni = token.to_unicode()
                if token_uni == 'UPDATE' :
                    self.update_loc = i
                elif token_uni == 'SET' :
                    self.set_loc = i
                ## ENDIF
            elif cls == 'Where' :
                self.where_loc = i
            i += 1
            ## ENDIF
        ## ENDFOR
        
        ''' Determine table name '''
        tbl_token_loc = self.update_loc + 2
        cls = self.stmt.tokens[tbl_token_loc].__class__.__name__
        if cls == 'Identifier' :
            tbl_name = self.stmt.tokens[tbl_token_loc].to_unicode()
            self.tables['main'] = tbl_name
            self.where_cols['main'] = []
            self.select_cols['main'] = []
            self.sort_cols['main'] = []
            self.limit['main'] = None
            self.skip['main'] = None
        ## ENDIF
        
        ''' PROCESS SET clause '''
        if self.set_loc <> None :
            end = len(self.stmt.tokens) - 1
            if self.where_loc <> None :
                end = self.where_loc
            ## ENDIF
            for i in range(self.set_loc + 1, end) :
                cls = self.stmt.tokens[i].__class__.__name__
                if cls == 'Comparison' :
                    parts = self.process_where_comparison(self.stmt.tokens[i])
                    self.select_cols[parts[0]].append(parts[1])
                ## ENDIF
            ## ENDFOR
            
        ''' PROCESS WHERE clause '''
        if self.where_loc <> None :
            where = self.stmt.tokens[self.where_loc]
            count = len(where.tokens)
            i = 1
            while i < count :
                if where.tokens[i].ttype == sqlparse.tokens.Whitespace :
                    pass 
                else :
                    cls = where.tokens[i].__class__.__name__
                    if cls == 'Token' :
                        pass
                    elif cls == 'Identifier' :
                        offset = 1;
                        if where.tokens[i + 1].ttype == sqlparse.tokens.Whitespace :
                            offset = 2;
                        parts = self.process_where_comparison_separate(where.tokens[i], where.tokens[i+offset], where.tokens[i+offset+offset])
                        self.where_cols[parts[0]].append(parts[1])
                        i += 4
                    elif cls == 'Comparison' :
                        parts = self.process_where_comparison(where.tokens[i])
                        self.where_cols[parts[0]].append(parts[1])
                    ## ENDIF
                ## ENDIF
                i += 1
            ## ENDWHILE
        ## ENDIF
        
        print self.where_cols
        print self.select_cols
    ## ENDDEF
    
    '''
    Process the SQL Statment
    '''
    def process(self) : 
        parsed = sqlparse.parse(self.sql)
        if len(parsed) == 0 :
            self.errors.append('Conversion error')
        else :
            self.stmt = parsed[0]
            self.query_type = self.stmt.get_type()
            if self.query_type == 'SELECT' :
                self.do_select()
            elif self.query_type == 'INSERT' :
                self.do_insert()
            elif self.query_type == 'DELETE' :
                self.do_delete()
            elif self.query_type == 'UPDATE' :
                self.do_update()
            else :
                self.errors.append('Invalid query type')
            ## ENDIF
        ## ENDIF
    ## ENDDEF

    '''
    Process an identifier to determine if an alias has been used
    '''
    def process_identifier(self, token) :
        parts = token.to_unicode().split('.')
        if len(parts) == 1 :
            return ('main', parts[0])
        else :
            return (parts[0], parts[1])
        ## ENDIF
    ### ENDDEF

    '''
    Convert the WHERE clause to the appropriate formatting for one condition
    '''
    def process_where_clause(self, attr, op, value) :
        if op == ':' :
            return attr + op + value
        elif op == 'LIKE' :
            value = value.strip('"\'')
            result = attr + ':/'
            if value[0] <> '%' :
                result += '^'
            ## ENDIF
            result += value.strip('%')
            if value[len(value) - 1] <> '%' :
                result += '^'
            ## ENDIF
            return result + '/}'
        else :
            return attr + ':' + '{' + op + ':' + value + '}'
        ## ENDIF
    ## ENDDEF
    
    '''
    '''
    def process_where_comparison(self, comp) :
        tokens = self.strip_whitespace(comp)
        parts = self.process_identifier(tokens[0])
        clause = self.process_where_clause(parts[1], self.process_where_comparison_operator(tokens[1]), self.process_where_comparison_value(tokens[2]))
        return (parts[0], clause)
    ## ENDDEF
    
    '''
    Convert SQL comparison operators to MongoDB comparison operators
    '''
    def process_where_comparison_operator(self, op) :
        cls = op.__class__.__name__
        if cls == 'Token' :
            if op.to_unicode() == '=' :
                return ':'
            elif op.to_unicode() == '>' :
                return '$gt'
            elif op.to_unicode() == '>=' :
                return '$gte'
            elif op.to_unicode() == '<' :
                return '$lt'
            elif op.to_unicode() == '<=' :
                return '$lte'
            elif op.to_unicode() == '!=' :
                return '$ne'
            elif op.to_unicode() == 'LIKE' :
                return 'LIKE'
            else :
                return '?'
            ## ENDIF
        else :
            return '?'
        ## ENDIF
    ## ENDDEF
    
    '''
    Process a where comparison specified by individual Tokens
    '''
    def process_where_comparison_separate(self, ident, comp, value) :
        parts = self.process_identifier(ident)
        operator = self.process_where_comparison_operator(comp)
        str = self.process_where_clause(parts[1], operator, self.process_where_comparison_value(value))
        return (parts[0], str)
    ## ENDDEF
    
    '''
    Process a where comparison value
    '''
    def process_where_comparison_value(self, value) :
        cls = value.__class__.__name__
        if cls == 'Identifier' :
            return "'" + value.to_unicode().strip('"\'') + "'"
        elif value.ttype == sqlparse.tokens.String :
            return "'" + value.to_unicode().strip('"\'') + "'"
        else :
            return value.to_unicode()
        ## ENDIF
    ## ENDDEF

    '''
    Produce a list of Mongo commands derived from this SQL statement
    '''
    def render(self, db='db') :
        if self.query_type == 'SELECT' :
            results = []
            for alias, table_name in self.tables.iteritems() :
                results.append(self.compose_mongo(db, table_name, alias, 'find'))
            ## ENDFOR
            return results
        elif self.query_type == 'INSERT' :
            results = []
            for alias, table_name in self.tables.iteritems() :
                results.append(self.compose_mongo(db, table_name, alias, 'insert'))
            ## ENDFOR
            return results
        elif self.query_type == 'DELETE' :
            results = []
            for alias, table_name in self.tables.iteritems() :
                results.append(self.compose_mongo(db, table_name, alias, 'remove'))
            ## ENDFOR
            return results
        elif self.query_type == 'UPDATE' :
            results = []
            for alias, table_name in self.tables.iteritems() :
                results.append(self.compose_mongo(db, table_name, alias, 'update'))
            ## ENDFOR
            return results
        else :
            return None
        ## ENDIF
    ## ENDDEF
    
    '''
    Remove white space tokens from a TokenList
    '''
    def strip_whitespace(self, tokenlist) :
        newlist = []
        for token in tokenlist.tokens :
            if token.ttype == sqlparse.tokens.Whitespace :
                pass
            else :
                newlist.append(token)
            ## ENDIF
        ## ENDFOR
        return newlist
    ## ENDDEF
## ENDCLASS