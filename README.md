# Hadoop Project
Name: Janet Chavez Erazo
Email: jchav026@ucr.edu
Student ID: 861093910

This project was done by myself, and all contributions were made by me. As a beginner to Hadoop and Javascript, i decided to create one java file, containing 2 mappers to seperate the weatherlocations csv file, and all the txt files. The java file also includes a reducer, and a driver.

The first mapper: reads the csv file, removes headers, and filters columns to the first 4 columns 
The second mapper: this mapper is looking for all the txt files whose string contains ".txt" instead of having multiple mappers to read each file
The reduce function:

On windows command line, I ran the following functions to import my files in a directory:
```
>cd C:/hadoop/sbin
>start-dfs
>start-yarn
>hadoop fs -mkdir /input
>hadoop fs -mkdir /output
>hadoop fs -put C:/weatherstationlocations.csv /input
>hadoop fs -ls /input/ #to confirm file is in the directory. performed the same for txt files 
```

And ran the following command to read the java file:
```
>hadoop jar C:/weatherproject.jar /input /output
```


    
