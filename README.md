## Using Testing Infrastructure

### Pushing to Online Tester Repos

1. `git add remote gr10 https://github.com/ds-uot/ms2-group-10`
2. `git push gr10 <BRANCH>`

Note: 
- The online tester clones directly from `main` in the group-10 repo, so make sure to push to `main` in the group-10 repo
- Ideally this can be done by merging your branch to `main` in the `ece419` repo and then pushing to `main` in the group-10 repo

### SQL Operations

The following commands we're added in our milestone 4 feature extension:

##### Creating a Table

`sqlcreate <tablename> <schema>`

    –– creates an empty table with the specified schema
    –– the first column in the schema is taken to be the primary key for the table
        
    e.g.
    ```
    sqlcreate People name:text
    sqlcreate School student:text,age:int
    ```

##### Inserting rows to a table

`sqlinsert <tablename> <row>`
    
    –– inserts a JSON-specified row into tablename
    
    e.g.
    ```
    sqlinsert People {“name":Steve}
    sqlinsert School {"age":12,"student":Jason}
    sqlinsert School {“student":Jason,"age":12}
    ```


##### Updating existing rows in a table

`sqlupdate <tablename> <row>`
    
    –– update a JSON-specified row in tablename
    
    e.g.
    ```
    sqlupdate People {“name":Steve}
    sqlupdate School {"age":12,"student":Jason}
    ```

##### Querying a table (i.e. a select command)

`sqlselect <tablename>`
    
    –– select all columns & all rows in tablename
    
    e.g.
    ```
    sqlselect People
    ```


`sqlselect <columns> from <tablename> where <conditions>`
   
    –– select the specified columns (inc. all via *) & rows matching the conditions in tablename
    
    e.g.
    ```
    sqlselect * from People
    sqlselect {student,age} from School 
    sqlselect {student} from School where {age>7,student=Jason}
    ```

##### Dropping an existing table

`sqldrop <tablename>` 
    
    –– drop tablename from database
    
    e.g.
    ```
    sqldrop School
    ```
