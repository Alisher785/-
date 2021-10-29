# -
Возьмем два файла с разными форматами о таксофонах Москвы и интегрируемых их в единый документ.

Данные:
1)https://data.gov.ru/opendata/7710878000-taxofony
Атрибуты: "global_id","signature_date","system_object_id","ID","Name","Latitude_WGS84","District","DescriptionLocation",
	  "PayWay","IntercityConnectionPayment","ValidUniversalServicesCard","Longitude_WGS84","AdmArea"

2)https://data.mos.ru/opendata/7710878000-taksofony-mgts?pageNumber=4&versionNumber=3&releaseNumber=12
Атрибуты: global_id, ID, AdmArea, District, DescriptionLocation, PayWay, IntercityConnectionPayment, ValidUniversalServicesCard
Все атрибуты, присутствующие в .xlsx файле, можно найти и в файле .json. Но при этом в .json файле есть 4 уникальных атрибута.



Первый шаг:
Вручную добавим новый атрибут в .xlsx файл, связанную с датой, в формате отличающемся от формата
данных в .json файле. При создание целевой схемы уберем некоторые атрибуты из .json
файла, чтобы уменьшить количество столбцов, так как они не представляют никакого интереса.
Объединим три атрибута 'AdmArea','District', 'DescriptionLocation' в один атрибут 'Address'
и в дальнейшем будем использовать его как ключ.

После отработки кода, выберем только небольшую часть данных, чтобы лучше отслежить работу
поиска дубликатов, так как слишком большая выборка не имеет различий(помимо введенной мной ранее даты). 
Поэтому также введем некоторые различия, которые будем решать на этапе data Fusing.

Целевая схема(формат .json):  
Атрибуты:'signature_date', 'Name', 'PayWay', 'Address', 'ValidUniversalServicesCard', 
		'ID', 'IntercityConnectionPayment', 'global_id', 'Latitude_WGS84'
    
Второй шаг: 
Теперь у нас есть два файла единого формата и с единой целевой схемой. Обработаем эти файлы и найдем дублирующие данные, использую модуль MapReduce. Для поиска дубликатов будем использовать хэш по уникальному ключу(в нашем случае часть адреса - округ). При нахождении дубликатов будем помечать их единым duplicate_id, атрибут который мы добавим для каждого объекта. В выходной файл запишем все дубликаты.


Третий шаг: 
Проведем слияние данных, используя модуль MapReduce. Для дубликатов выделем решающие функции в программе, чтобы правильно слить дубликаты. В качестве результата создадим файл, который будет содержать записи о каждом реальном объекте без дубликатов.  
