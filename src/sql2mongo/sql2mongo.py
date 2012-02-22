# -*- coding: utf-8 -*-

import sqlparse
import json
import yaml

'''
@todo: JOIN processing
@todo: Process basic OR criteria
@todo: Handle nested queries?
'''

class Sql2mongo (object) :

    '''
    Class constructor
    '''
    def __init__(self, schema = {}) :
        self.schema = schema
        self.reset()
    ## End __init__()
    
    '''
    Take a tuple specifying the attribute, operator, and value and add to the structure
    defining the Where clause
    '''
    def add_where_comparison(self, table_alias, tuple) :
        tbl_name = self.table_aliases[table_alias]
        columns = list(self.where_cols[tbl_name])
        if tuple[0] in columns :
            pass
        else :
            self.where_cols[tbl_name][tuple[0]] = []
        self.where_cols[tbl_name][tuple[0]].append((tuple[1], tuple[2]))
    ## End add_where_comparison()
    
    def generate_content_insert(self, table) :
        return self.render_trace_where_clause(table)
    ## End generate_content_insert()

    def generate_content_query(self, table) :
        query_dict = self.render_trace_where_clause(table)
        return {u'query': query_dict}
    ## End generate_content_query()

    def generate_content_remove(self, table) :
        return self.render_trace_where_clause(table)
    ## End generate_content_remove()

    def generate_content_update(self, table) :
        query_dict = self.render_trace_where_clause(table)
        set_dict = self.render_trace_set_clause(table)
        return [query_dict, set_dict]
    ## End generate_content_update()

    def generate_operations(self, timestamp) :
        operations = []
        for alias, table in self.table_aliases.iteritems() :
            op = {}
            op['collection'] = table
            op['timestamp'] = timestamp
            op['content'] = []
            op['type'] = self.mongo_type()
            op['size'] = 0
            if self.query_type == 'DELETE' :
                op['content'].append(self.generate_content_remove(table))
            elif self.query_type == 'INSERT' :
                op['content'].append(self.generate_content_insert(table))
            elif self.query_type == 'SELECT' :
                op['content'].append(self.generate_content_query(table))
            elif self.query_type == 'UPDATE' :
                content = self.generate_content_update(table)
                for i in content :
                    op['content'].append(i)
        return operations
    ## End generate_operations()
        
    def mongo_type(self) :
        if self.query_type == 'DELETE' :
            return u'$remove'
        elif self.query_type == 'INSERT' :
            return u'$insert'
        elif self.query_type == 'SELECT' :
            return u'$query'
        elif self.query_type == 'UPDATE' :
            return u'$update'
        else :
            return None
    ## End mongo_type()
    
    '''
    Process an identifier to determine if an alias has been used
    '''
    def process_identifier(self, token) :
        parts = token.to_unicode().split('.')
        if len(parts) == 1 :
            return ('main', parts[0])
        else :
            return (parts[0], parts[1])
    ## End process_identifier()

    def process_query_delete(self) :
        delete_loc, where_loc = None, None
        i = 0
        for token in self.stmt.tokens :
            cls = token.__class__.__name__
            if cls == 'Token' :
                token_uni = token.to_unicode()
                if token_uni == 'DELETE' :
                    delete_loc = i
                elif token_uni == 'FROM' :
                    delete_loc = i
            elif cls == 'Where' :
                where_loc = i
            i += 1
        
        ''' Determine table name '''
        tbl_token_loc = delete_loc + 2
        cls = self.stmt.tokens[tbl_token_loc].__class__.__name__
        if cls == 'Identifier' :
            tbl_name = self.stmt.tokens[tbl_token_loc].to_unicode()
            self.table_aliases['main'] = tbl_name
        
        ''' PROCESS WHERE clause '''
        if where_loc <> None :
            where = self.stmt.tokens[where_loc]
            count = len(where.tokens)
            i = 1
            while i < count :
                if where.tokens[i].ttype == sqlparse.tokens.Whitespace :
                    pass 
                elif where.tokens[i].ttype == sqlparse.tokens.Keyword :
                    if where.tokens[i].to_unicode().lower() == u'or' :
                        self.use_or = True
                else :
                    cls = where.tokens[i].__class__.__name__
                    if cls == 'Token' :
                        pass
                    elif cls == 'Identifier' :
                        offset = 1;
                        if where.tokens[i + 1].ttype == sqlparse.tokens.Whitespace :
                            offset = 2;
                        parts = self.process_where_comparison_separate(where.tokens[i], where.tokens[i+offset], where.tokens[i+offset+offset])
                        self.add_where_comparison(parts[0], parts[1])
                        i += 4
                    elif cls == 'Comparison' :
                        parts = self.process_where_comparison(where.tokens[i])
                        self.add_where_comparison(parts[0], parts[1])
                i += 1
    ## End process_query_delete()

    def process_query_insert(self) :
        insert_loc, values_loc = None, None
        i = 0
        for token in self.stmt.tokens :
            cls = token.__class__.__name__
            if cls == 'Token' :
                token_uni = token.to_unicode()
                if token_uni == 'INSERT' :
                    insert_loc = i
                elif token_uni == 'INTO' :
                    insert_loc = i
                elif token_uni == 'VALUES' :
                    values_loc = i
            i += 1
        
        ''' Determine table name and columns '''
        tbl_token_loc = insert_loc + 2
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
                elif cls == 'Identifier' :
                    cols.append(token.to_unicode())
                elif cls == 'Token' and token.ttype == sqlparse.tokens.Token.Name.Builtin :
                    cols.append(token.to_unicode())
        elif cls == 'Identifier' :
            tbl_name = self.stmt.tokens[tbl_token_loc].to_unicode()
            cols_specified = False
            cols = self.schema[tbl_name]
        
        ''' Determine column values '''
        column_values = []
        values_loc = values_loc + 2
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
        
        self.table_aliases['main'] = tbl_name
        
        i = 0
        for col in column_values :
            self.add_where_comparison('main', (cols[i], ':', col))
            i += 1
    ## End process_query_insert()

    def process_query_select(self) :
        ''' Find the location of various clauses'''
        select_loc, from_loc, where_loc, sort_loc, limit_loc, skip_loc,  = None, None, None, None, None, None
        i = 0
        for token in self.stmt.tokens :
            cls = token.__class__.__name__
            if cls == 'Token' :
                token_uni = token.to_unicode()
                if token_uni == 'SELECT' :
                    select_loc = i
                elif token_uni == 'FROM' :
                    from_loc = i
                elif token_uni == 'LIMIT' :
                    limit_loc = i
                elif token_uni == 'ORDER' :
                    sort_loc = i
                elif token_uni == 'BY' :
                    sort_loc = i
                elif token_uni == 'SKIP' :
                    skip_loc = i
                else :
                    pass
            elif cls == 'Identifier' :
                if token.to_unicode() == 'SKIP' :
                    skip_loc = i
                ## End if
            elif cls == 'IdentifierList' :
                pass
            elif cls == 'Where' :
                where_loc = i
            else :
                pass
            i += 1
        
        ''' Process FROM clause '''
        if where_loc <> None :
            end = where_loc
        elif sort_loc <> None :
            end = sort_loc
        elif limit_loc <> None :
            end = limit_loc
        elif skip_loc <> None :
            end = skip_loc
        else :
            end = len(self.stmt.tokens)
        ## End if
        
        if from_loc == None :
            self.query_type == 'UNKNOWN'
            return None
        ## End if
        
        for i in range(from_loc + 1, end) :
            cls = self.stmt.tokens[i].__class__.__name__
            if cls <> 'Token' :
                parts = self.stmt.tokens[i].to_unicode().split(' ')
                if len(parts) == 1 :
                    index = 'main'
                elif len(parts) == 2 :
                    index = parts[1]
                else :
                    index = parts[2]
                ## End if
                self.table_aliases[index] = parts[0]
            ## End if
        ## End for
        
        ''' Process Joins '''
        
        ''' Process SELECT Clause '''
        for i in range(select_loc + 1, from_loc) :
            cls = self.stmt.tokens[i].__class__.__name__
            if cls == 'Token' :
                pass
            elif cls == 'IdentifierList' :
                ilist = self.stmt.tokens[i].get_identifiers()
                for il in ilist :
                    parts = il.to_unicode().split('.')
                    if len(parts) == 1 :
                        self.project_cols[self.table_aliases['main']].append(parts[0])
                    else :
                        self.project_cols[self.table_aliases[parts[0]]].append(parts[1])
            else :
                parts = self.stmt.tokens[i].to_unicode().split('.')
                if len(parts) == 1 :
                    self.project_cols[self.table_aliases['main']].append(parts[0])
                else :
                    self.project_cols[self.table_aliases[parts[0]]].append(parts[1])
        
        ''' Process WHERE Clause '''
        if where_loc <> None :
            where = self.stmt.tokens[where_loc] 
            count = len(where.tokens)
            i = 1
            while i < count :
                if where.tokens[i].ttype == sqlparse.tokens.Whitespace :
                    pass
                elif where.tokens[i].ttype == sqlparse.tokens.Keyword :
                    if where.tokens[i].to_unicode().lower() == u'or' :
                        self.use_or = True
                    ## End if
                else :
                    cls = where.tokens[i].__class__.__name__
                    if cls == 'Token' :
                        pass
                    elif cls == 'Identifier' :
                        parts = self.process_where_comparison_separate(where.tokens[i], where.tokens[i+2], where.tokens[i+4])
                        self.add_where_comparison(parts[0], parts[1])
                        i += 4
                    elif cls == 'Comparison' :
                        parts = self.process_where_comparison(where.tokens[i])
                        self.add_where_comparison(parts[0], parts[1])
                    ## ENDIF
                ## ENDIF
                i += 1
            ## ENDWHILE
        ## ENDIF
        
        ''' Process ORDER BY '''
        if sort_loc <> None :
            if limit_loc <> None :
                end = limit_loc
            elif skip_loc <> None :
                end = skip_loc
            else :
                end = len(self.stmt.tokens)
            
            i = sort_loc + 1
            while i < end :
                if self.stmt.tokens[i].ttype == sqlparse.tokens.Whitespace :
                    pass
                else :
                    cls = self.stmt.tokens[i].__class__.__name__
                    if cls == 'Identifier' :
                        parts = self.process_identifier(self.stmt.tokens[i])
                        if i + 2 > end :
                            sort = '1'
                        elif self.stmt.tokens[i + 2].ttype == sqlparse.tokens.Token.Punctuation :
                            sort = '1'
                        else :
                            if self.stmt.tokens[i + 2].value == 'ASC' :
                                sort = '1'
                            else :
                                sort = '-1'
                            ## ENDIF
                        self.sort_cols[self.table_aliases[parts[0]]].append('{' + parts[1] + ':' + sort + '}')
                        i += 2
                i += 1
        
        ''' PROCESS LIMIT '''
        if limit_loc <> None :
            for alias, table_name in self.table_aliases.iteritems() :
                self.limit[table_name] = self.stmt.tokens[limit_loc + 2].to_unicode()
        
        ''' PROCESS SKIP '''
        if skip_loc <> None :
            for alias, table_name in self.table_aliases.iteritems() :
                self.skip[table_name] = self.stmt.tokens[skip_loc + 2].to_unicode()
    ## End process_query_select()

    def process_query_update(self) :
        update_loc, set_loc, where_loc = None, None, None
        i = 0
        for token in self.stmt.tokens :
            cls = token.__class__.__name__
            if cls == 'Token' :
                token_uni = token.to_unicode()
                if token_uni == 'UPDATE' :
                    update_loc = i
                elif token_uni == 'SET' :
                    set_loc = i
            elif cls == 'Where' :
                where_loc = i
            i += 1
        
        ''' Determine table name '''
        tbl_token_loc = update_loc + 2
        cls = self.stmt.tokens[tbl_token_loc].__class__.__name__
        if cls == 'Identifier' :
            tbl_name = self.stmt.tokens[tbl_token_loc].to_unicode()
            self.table_aliases['main'] = tbl_name
        
        ''' PROCESS SET clause '''
        if set_loc <> None :
            end = len(self.stmt.tokens) - 1
            if where_loc <> None :
                end = where_loc
            for i in range(set_loc + 1, end) :
                cls = self.stmt.tokens[i].__class__.__name__
                if cls == 'Comparison' :
                    parts = self.process_where_comparison(self.stmt.tokens[i])
                    self.set_cols[self.table_aliases[parts[0]]].append(parts[1])
            
        ''' PROCESS WHERE clause '''
        if where_loc <> None :
            where = self.stmt.tokens[where_loc]
            count = len(where.tokens)
            i = 1
            while i < count :
                if where.tokens[i].ttype == sqlparse.tokens.Whitespace :
                    pass 
                elif where.tokens[i].ttype == sqlparse.tokens.Keyword :
                    if where.tokens[i].to_unicode().lower() == u'or' :
                        self.use_or = True
                    ## ENDIF
                else :
                    cls = where.tokens[i].__class__.__name__
                    if cls == 'Token' :
                        pass
                    elif cls == 'Identifier' :
                        offset = 1;
                        if where.tokens[i + 1].ttype == sqlparse.tokens.Whitespace :
                            offset = 2;
                        parts = self.process_where_comparison_separate(where.tokens[i], where.tokens[i+offset], where.tokens[i+offset+offset])
                        self.add_where_comparison(parts[0], parts[1])
                        i += 4
                    elif cls == 'Comparison' :
                        parts = self.process_where_comparison(where.tokens[i])
                        self.add_where_comparison(parts[0], parts[1])
                i += 1
    ## End process_query_update()
    
    def process_sql(self, sql, reset=True) :
        if reset == True :
            self.reset()
        parsed = sqlparse.parse(sql)
        if len(parsed) > 0 :
            self.stmt = parsed[0]
            self.query_type = self.stmt.get_type()
            if self.query_type == 'SELECT' :
                self.process_query_select()
            elif self.query_type == 'INSERT' :
                self.process_query_insert()
            elif self.query_type == 'DELETE' :
                self.process_query_delete()
            elif self.query_type == 'UPDATE' :
                self.process_query_update()
            else :
                self.query_type = 'UNKNOWN'
    ## End process_sql()
    
    def process_where_clause(self, attr, op, value) :
        if op == 'LIKE' :
            op = ':'
            value = value.strip('"\'')
            if value[0] == '%' :
                value = '/' + value.lstrip('%') 
            else :
                value = '^' + value
            if value[len(value) - 1] == '%' :
                value = value.rstrip('%') + '/'
            else :
                value += '^'
        return (attr, op, value)
    ## End process_where_clause()
    
    def process_where_comparison(self, comp) :
        tokens = self.strip_whitespace(comp)
        parts = self.process_identifier(tokens[0])
        clause = self.process_where_clause(parts[1], self.process_where_comparison_operator(tokens[1]), self.process_where_comparison_value(tokens[2]))
        return (parts[0], clause)
    ## End process_where_comparison()
    
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
        else :
            return '?'
    ## End process_where_comparison_operator()
    
    '''
    Process a where comparison specified by individual Tokens
    '''
    def process_where_comparison_separate(self, ident, comp, value) :
        parts = self.process_identifier(ident)
        operator = self.process_where_comparison_operator(comp)
        str = self.process_where_clause(parts[1], operator, self.process_where_comparison_value(value))
        return (parts[0], str)
    ## End process_where_comparison_separate()
    
    def process_where_comparison_value(self, value) :
        cls = value.__class__.__name__
        if cls == 'Identifier' :
            return "'" + str(value.to_unicode()).strip('"\'') + "'"
        elif value.ttype == sqlparse.tokens.String :
            return "'" + str(value.to_unicode()).strip('"\'') + "'"
        else :
            return value.to_unicode()
    ## End process_where_comparison_value()
    
    '''
    Reset internal data to initial state
    '''
    def reset(self) :
        self.query_type = None
        self.stmt = None
        self.use_or = False
        self.limit = {}
        self.project_cols = {}
        self.skip = {}
        self.set_cols = {}
        self.sort_cols = {}
        self.table_aliases = {}
        self.where_cols = {}
        for table, columns in self.schema.iteritems() :
            self.where_cols[table] = {}
            self.project_cols[table] = []
            self.set_cols[table] = []
            self.sort_cols[table] = []
            self.limit[table] = None
            self.skip[table] = None
    ## End reset()
    
    '''
    Output data as a standard mongo command
    '''
    def render_mongo_command(self) :
        if self.query_type == 'DELETE' :
            return self.render_mongo_remove()
        elif self.query_type == "INSERT" :
            return self.render_mongo_insert()
        elif self.query_type == "SELECT" :
            return self.render_mongo_query()
        elif self.query_type == "UPDATE" :
            return self.render_mongo_update()
        else :
            return None
    ## End render_mongo_command()
    
    def render_mongo_insert(self) :
        output = []
        for alias, table in self.table_aliases.iteritems() :
            mongo = 'db.' + table + '.insert('
            mongo += self.render_mongo_where_clause(table)
            mongo += ')'
            output.append(unicode(mongo))
        return output
    ## End render_mongo_insert()
    
    def render_mongo_project_clause(self, tbl_name) :
        temp = []
        for col in self.project_cols[tbl_name] :
            temp.append(col + ':1')
        return '{' + ','.join(temp) + '}'
    ## End render_mongo_project_clause
    
    def render_mongo_query(self) :
        output = []
        for alias, table in self.table_aliases.iteritems() :
            mongo = 'db.' + table + '.find('
            mongo += self.render_mongo_where_clause(table)
            if len(self.project_cols[table]) > 0 :
                mongo += ', ' + self.render_mongo_project_clause(table)
            mongo += ')'
            if len(self.sort_cols[table]) > 0 :
                mongo += '.sort(' + ','.join(self.sort_cols[table]) + ')'
            if self.limit[table]  <> None :
                mongo += '.limit(' + self.limit[table] + ')'
            if self.skip[table] <> None :
                mongo += '.skip(' + self.skip[table] + ')'
            output.append(unicode(mongo))
        return output
    ## End render_mongo_query()
        
    def render_mongo_set_clause(self, tbl_name) :
        parts = []
        for tuple in self.set_cols[tbl_name] :
            parts.append('$set:{' + tuple[0] + ':' + tuple[2] + '}')
        return '{' + ','.join(parts) + '}'
    ## End render_mongo_set_clause()
    
    def render_mongo_remove(self) :
        output = []
        for alias, table in self.table_aliases.iteritems() :
            mongo = 'db.' + table + '.remove('
            mongo += self.render_mongo_where_clause(table)
            mongo += ')'
            output.append(unicode(mongo))
        return output
    ## End render_mongo_remove()
    
    def render_mongo_update(self) :
        output = []
        for alias, table in self.table_aliases.iteritems() :
            mongo = 'db.' + table + '.update('
            mongo += self.render_mongo_where_clause(table)
            mongo += ', '
            mongo += self.render_mongo_set_clause(table)
            mongo += ', false, true)'
            output.append(unicode(mongo))
        return output
    ## End render_mongo_update()
    
    '''
    Translate the elements of the where clause to the appropriate Mongo DB sub-command
    '''
    def render_mongo_where_clause(self, tbl_name) :
        if (self.use_or == True) :
            parts = []
            for col, ops in self.where_cols[tbl_name].iteritems() :
                if len(ops) == 1 :
                    if ops[0][0] == ':' :
                            cmd = col + ops[0][0] + ops[0][1]
                    else :
                        cmd = col + ':{' + ops[0][0] + ':' + ops[0][1] + '}'
                    parts.append(cmd)
                else :
                    inner_parts = []
                    for tups in ops :
                        inner_parts.append(tups[0] + ':' + tups[1])
                    parts.append('\'' + col + '\':{' + ','.join(inner_parts) + '}')
            return '{$or:[{' + '},{'.join(parts) + '}]}'
        else :
            if len(self.where_cols[tbl_name]) > 0 :
                parts = []
                for col, ops in self.where_cols[tbl_name].iteritems() :
                    if len(ops) == 1 :
                        if ops[0][0] == ':' :
                            cmd = col + ops[0][0] + ops[0][1]
                        else :
                            cmd = col + ':{' + ops[0][0] + ':' + ops[0][1] + '}'
                        parts.append(cmd)
                    else :
                        inner_parts = []
                        for tups in self.where_cols[tbl_name][col] :
                            inner_parts.append(tups[0] + ':' + tups[1])
                        parts.append('\'' + col + '\':{' + ','.join(inner_parts) + '}')
                return '{' + ','.join(parts) + '}'
            elif len(self.project_cols[tbl_name]) > 0 :
                return '{}'
            else :
                return ''
    ## End render_mongo_where_clause()
    
    def render_trace(self) :
        if self.query_type == 'DELETE' :
            return self.render_trace_remove()
        elif self.query_type == "INSERT" :
            return self.render_trace_insert()
        elif self.query_type == "SELECT" :
            return self.render_trace_query()
        elif self.query_type == "UPDATE" :
            return self.render_trace_update()
        else :
            return None
    ## End render_trace()
       
    def render_trace_insert(self) :
        output = []
        for alias, table in self.table_aliases.iteritems() :
            insert_dict = self.render_trace_where_clause(table)
            output.append(insert_dict)
        return output
    ## End render_trace_insert()
    
    def render_trace_query(self) :
        output = []
        for alias, table in self.table_aliases.iteritems() :
            query_dict = self.render_trace_where_clause(table)
            dict = {u'query': query_dict}
            output.append(dict)
        return output
    ## End render_trace_query()
    
    def render_trace_remove(self) :
        output = []
        for alias, table in self.table_aliases.iteritems() :
            query_dict = self.render_trace_where_clause(table)
            output.append(query_dict)
        return output
    ## End render_trace_remove()
    
    def render_trace_set_clause(self, table) :
        output = {}
        for col in self.set_cols[table] :
            output[col[0]] = self.render_trace_value(col[2])
        return output
    ## End render_trace_set_clause()
    
    def render_trace_update(self) :
        output = []
        for alias, table in self.table_aliases.iteritems() :
            query_dict = self.render_trace_where_clause(table)
            output.append(query_dict)
            set_dict = self.render_trace_set_clause(table)
            output.append(set_dict)
        return output
    ## End render_trace_update()
    
    def render_trace_value(self, val) :
        if val[0] in ['"', "'"] :
            val = val.strip('"\'')
        else :
            val = float(val)
        return val
    ## End render_trace_value()
    
    def render_trace_where_clause(self, tbl_name) :
        output = {}
        if (self.use_or == True) :
            clauses = []
            for col, ops in self.where_cols[tbl_name].iteritems() :
                part = {}
                if len(ops) == 1 :
                    if ops[0][0] == ':' :
                        part[col] = self.render_trace_value(ops[0][1])
                    else :
                        dict = {}
                        dict[ops[0][0]] = self.render_trace_value(ops[0][1])
                        part[col] = dict
                else :
                    inner_parts = {}
                    for tups in ops :
                        inner_parts[tups[0]] = self.render_trace_value(tups[1])
                    part[col] = inner_parts
                clauses.append(part)
            return {'$or' : clauses }
        else :
            if len(self.where_cols[tbl_name]) > 0 :
                for col, ops in self.where_cols[tbl_name].iteritems() :
                    if len(ops) == 1 :
                        if ops[0][0] == ':' :
                            output[col] = self.render_trace_value(ops[0][1])
                        else :
                            dict = {}
                            dict[ops[0][0]] = self.render_trace_value(ops[0][1])
                            output[col] = dict
                    else :
                        inner_parts = {}
                        for tups in self.where_cols[tbl_name][col] :
                            inner_parts[tups[0]] = self.render_trace_value(tups[1])
                        output[col] = inner_parts
                return output
            elif len(self.project_cols[tbl_name]) > 0 :
                return {}
            else :
                return None
    ## End render_trace_where_clause()
    
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
        return newlist
    ## End strip_whitespace()
## End Sql2mongo class definition