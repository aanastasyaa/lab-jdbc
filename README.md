# lab-jdbc
Программа работает с БД с помощью **JDBC**. Используется БД	HSQLDB с двумя таблицами: ITEM (книги) и ITEMGROUP (авторы).  

Класс **_DBTester_** содержит методы по работе с БД, среди которых есть два метода (*changeItemsUseFile* и *changeGroupsUseFile*), 
редактирующие БД с помощью файлов *items.txt* и *groups.txt*.

Файл *createTables.sql* содержит скрипт создания таблиц и начального заполнения БД. 
