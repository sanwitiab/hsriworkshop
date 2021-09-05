# Connect to DB with R
# https://programminghistorian.org/en/lessons/getting-started-with-mysql-using-r#downloading-and-installing-mysql
# https://db.rstudio.com/best-practices/run-queries-safely/
# DB Management library
library(DBI)
library(odbc)
library(RMariaDB)
library(glue)
library(dplyr)
library(dbplyr)
library(ggplot2)

# Spatial data management library
library(sp)
library(rgdal)
library(rgeos)
library(sf)

conn <- dbConnect(MariaDB(),
  dbname = "hospital_big_data",
  username = "hospital_user",
  password = "Hospitalpassword_678!!",
  host = "103.86.49.12",
  post = "3306"
)

# List tables in DB
dbListTables(conn)
# List fileds
dbListFields(conn, "person")

# View samples data
dbGetQuery(conn, "SELECT * FROM person LIMIT 5")

# Person table
person <- dbGetQuery(conn, "SELECT HOSPCODE, PID, SEX, BIRTH, NATION, DISCHARGE, LABOR,
			   D_UPDATE FROM person")
head(person)
count(person)

hospital <- dbGetQuery(conn, "SELECT HOSPCODE, name, province,
                       district, subdistrict
                       from hospital")

# join person with hospital
person.h <- merge(x = hospital, y = person, by = "HOSPCODE")
str(person)
head(person.h)
# Read data with batches
query <- dbSendQuery(conn, "select * from surveillance")
result <- dbFetch(query, n = 20)
result
dbHasCompleted(query)
dbGetInfo(query)
# processed
# Separate year from datetime:
df.p <- person
df.p$BirthYear <- format(df.p$BIRTH, format = "%Y")
df.p$UpdateYear <- format(df.p$D_UPDATE, format = "%Y")

# Calc Age of Patient
df.p$age <- df.p$UpdateYear - df.p$BirthYear

# Plot with ggplot2
str(df.p)
df.p %>% count(DISCHARGE, sort = TRUE)

df.p %>% count(SEX, wt = DISCHARGE)

df.p %>%
  group_by(NATION) %>%
  tally(sort = T)

df.p %>% ggplot(., aes(DISCHARGE, ..count..)) +
  geom_bar(aes(fill = DISCHARGE), position = "dodge") +
  labs(title = "Patient status", x = "STATUS", y = "COUNT", color = "Legend Title\n") +
  scale_fill_discrete(labels = c("na", "ตาย", "ย้าย", "สาบสูญ", "ไม่จำหน่าย")) +
  theme(legend.position = "bottom")

df.p %>%
  filter(HOSPCODE == "10823") %>%
  ggplot(., aes(NATION, ..count..)) +
  geom_bar(aes(fill = SEX), position = "dodge")

# DIAG_IPD table
diag_ipd <- dbGetQuery(conn, "SELECT HOSPCODE, PID, DIAGTYPE, DIAGCODE,
			   D_UPDATE FROM diagnosis_ipd")
head(diag_ipd)
count(diag_ipd)

head(diag_opd %>% filter(DIAGTYPE == 1))

# DIAG_OPD table
diag_opd <- dbGetQuery(conn, "SELECT HOSPCODE, PID, DIAGTYPE, DIAGCODE,
			   D_UPDATE FROM diagnosis_opd")
head(diag_opd)
count(diag_opd)


# filter data
count(diag_ipd)
str(diag_ipd)
head(diag_ipd)
diag_ipd %>% filter(DIAGCODE == "U071")
dtype1 <- diag_ipd %>% filter(DIAGTYPE == 1)
count(dtype1)

# Join table with merge
#### Left Join using merge function
count(person)
count(diag_ipd)

###### with R base merge()
p.ipd <- merge(x = person, y = diag_ipd, by = "PID")
p.ipd
count(p.ipd)
##### with dplyr
p.ipd.dplyr <- person %>% inner_join(diag_ipd, by = "PID")
p.ipd.dplyr
count(p.ipd.dplyr)

# List unique value from column
dgclastList <- dbGetQuery(conn, "SELECT DISTINCT DIAGCODELAST
FROM surveillance")
dgclastList
as.character(dgclastList)
dgclist <- translate_sql(dgclastList)
dgclist

# get query with list of value
dbGetQuery(conn, "SELECT *
FROM surveillance
WHERE DIAGCODELAST IN('A009')")

# Query with GLUESQL
serveill_sql <- glue_sql("SELECT CONCAT(ILLCHANGWAT,ILLAMPUR,
ILLTAMBON) as ADMINID, YEAR(ILLDATE) as ILLYEAR,
DIAGCODELAST, COUNT(DIAGCODELAST) as DIAGCODELAST_N
FROM surveillance WHERE DIAGCODE  = ?
GROUP BY ADMINID, ILLYEAR, DIAGCODELAST
ORDER BY DIAGCODELAST_N DESC")

surveill <- dbSendQuery(conn, serveill_sql)

dbBind(surveill, list("A099"))

svg <- dbFetch(surveill)
head(svg)
str(svg)
dbClearResult(surveill)

svg %>% top_n(11)
# convert to interger
svg.int <- svg %>% mutate_if(bit64::is.integer64, as.numeric)
str(svg.int)

svg.int %>%
  filter(!DIAGCODELAST == "")

# split by year
svg_split <- split(svg.int, svg$ILLYEAR)
# svg.splityear <- svg %>% group_split(ILLYEAR)

# Access each element using the [[ operator like this:
svg_split[[12]]
# svg.splityear[[3]]


# Matching DF with spatial data
map <- readOGR("C:/Sanwit/HSRI/SHP_data/eec_tambon.shp")
str(map@data)

# join with polygon
map$DGA001_N <- svg_split[[12]][match(map$TAM_CODE, unique(svg_split[[12]]$ADMINID)), "DIAGCODELAST_N"]

# Write to shapefile
writeOGR(map, layer = "tb_diagcodelast_A099_2020", "C:/Test", driver = "ESRI Shapefile")

map$DGA001_N <- svg[match(map$TAM_CODE, unique(svg$ADMINID)), "DIAGCODELAST_N"]

# plot spatial for check with tmap
# https://cran.r-project.org/web/packages/tmap/vignettes/tmap-getstarted.html
library(tmap)
tm_shape(map) + tm_polygons("DGA001_N")

# Creating a wide table from a long table
# https://mgimond.github.io/ES218/Week03b.html
library(tidyr)
library(lubridate)
svg.int.wide <- pivot_wider(svg.int, names_from = ILLYEAR, values_from = DIAGCODELAST_N)
str(svg.int.wide)
