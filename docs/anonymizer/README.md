## Anonymizer 

The config.yaml file holds the tables and columns used for anonymization along with the new values.
You can utilize the tool by using the pre-set config file found in the git repository, or by creating your own.
The location where it is found is commercedbsync/resources/anonymizer/config.yml.
During the migration process, if the table and the column that is handled is part of the anonymizer configuration and the value is not part of the exclude list, then the configuration defined config.yml is going to be applied.

````
tables:
    - name: addresses -> name of the table that will be included in the process
        columns:
        - name: p_cellphone -> column name
          operation: REPLACE  -> operation that will be performed
          text: "{RANDOM(number,10)}" -> the new value
          exclude: ["some_string"] -> values to be excluded (i.e. not changed) 
          excludeRow: ["some_string"] -> optional, if value is matched for configured column anonymization for entire row is skipped and row is copied unaltered 
````

The 'exclude' clause is optional. You can use it if you wish to exclude certain values from anonymization. For instance, if there are test users for which you don't want to alter the values.
`name`, `operation` and `text` are mandatory.

Operations that can be performed on the data:
- REPLACE -> will replace the entire value
- APPEND -> will append a text to the current value
- DELETE -> will delete the value (set it to null)

Functions available:
- GUID -> generate a guid
- RANDOM -> generate a random value

The RANDOM function can have two parameters:
1. the type of random value: "number", "string", "city", "street"
2. length of the random value (this parameter is needed only for "number" and "string" types)


Text that can be used to replace/append to the value:
- simple string </br>
  text: "My description"


- random number with n digits "{RANDOM(number,n)}"</br>
  text: "{RANDOM(number,10)}"


- random string with n characters "{RANDOM(string,n)}"</br>
  text: "{RANDOM(string,3)}"


- GUID value</br>
  text: "{GUID}"


- any combination
  text: "{GUID}-{RANDOM(string,3)}-SOME_TEXT"


The absence of text will result in a blank field. If the configuration file is not correct an error will be displayed in the console.

