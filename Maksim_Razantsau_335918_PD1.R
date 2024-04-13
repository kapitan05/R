### Przetwarzanie Danych Ustrukturyzowanych 2024L
### Praca domowa nr. 3
###
### UWAGA:
### nazwy funkcji oraz ich parametrow powinny pozostac niezmienione.
###  
### Wskazane fragmenty kodu przed wyslaniem rozwiazania powinny zostac 
### zakomentowane
###

# -----------------------------------------------------------------------------#
# Wczytanie danych oraz pakietow.
# !!! Przed wyslaniem zakomentuj ten fragment
# -----------------------------------------------------------------------------#
install.packages(c('dplyr'))
install.packages(c('data.table'))
install.packages("data.table", dependencies = TRUE)
install.packages(c("compare"))
install.packages(c("microbenchmark"))
install.packages(c("lubridate"))

Posts <- read.csv("/Users/admin/Downloads/travel_stackexchange_com/Posts.csv.gz")
Users <- read.csv("/Users/admin/Downloads/travel_stackexchange_com/Users.csv.gz")
Votes <- read.csv("/Users/admin/Downloads/travel_stackexchange_com/Votes.csv.gz")
Comments <- read.csv("/Users/admin/Downloads/travel_stackexchange_com/Comments.csv.gz")
PostLinks <- read.csv("/Users/admin/Downloads/travel_stackexchange_com/PostLinks.csv.gz")

# -----------------------------------------------------------------------------#
# Zadanie 1
# -----------------------------------------------------------------------------#

sql_1 <- function(Users){
    # DataFrame Users grupujemy wedlug Year, Month
    # liczymy srednia Reputation oraz liczymy liczbe rekordow
    # wybieramy odpowiednie kolumny
    sqldf::sqldf("SELECT STRFTIME('%Y', CreationDate) AS Year,
            STRFTIME('%m', CreationDate) AS Month,
            COUNT(*) AS TotalAccountsCount,
            AVG(Reputation) AS AverageReputation
            FROM Users
            GROUP BY Year, Month")
}
sql_1(Users)

base_1 <- function(Users){
  # tworzymy kolumny Year, Month w Users
  Users$Year <- format(as.Date(Users$CreationDate), "%Y")
  Users$Month <- format(as.Date(Users$CreationDate), "%m")
  
  # agregujemy Users, zeby dostac TotalAccountsCount kolumne czyli liczbe Id dla konkretnego Year, Month
  result_count <- aggregate(Id ~ Year + Month, data = Users, FUN = length)
  
  # agregujemy Users, zeby dostac AverageReputation
  result_avg_rep <- aggregate(Reputation ~ Year + Month, data = Users, FUN = function(x) mean(x, na.rm = TRUE))
  
  #laczymy result_count i result_avg_rep 
  aggregated_data <- merge(result_count, result_avg_rep, by = c("Year", "Month"))
  
  # nadajemy pozadane nazwy kolumnom
  colnames(aggregated_data) <- c('Year', "Month", "TotalAccountsCount", "AverageReputation")
  return (aggregated_data);
}

dplyr_1 <- function(Users){
    
    Users |>
    dplyr::select(CreationDate, Reputation) |> # wybierany z Users potrzebne kolumny 
    dplyr::mutate(Month = format(as.Date(CreationDate), "%m"), Year = format(as.Date(CreationDate), "%Y")) |> # tworzymy Month/Year 
    dplyr::group_by(Year, Month) |>
    # grupujemy wedlug Year/Month, tworzac AverageReputation TotalAccountsCount
    dplyr::summarize(TotalAccountsCount = dplyr::n(), AverageReputation = mean(Reputation, na.rm = TRUE)) |>
    
    # zmieniamy na class data.frame
    as.data.frame()
}
dplyr_1(Users)

table_1 <- function(Users) {
  # konwertacja na data.table
  DT <- data.table::data.table(Users)
  
  # wybierany z Users potrzebne kolumny 
  DT <- DT[, .(CreationDate, Reputation)]
  
  # tworzymy Year/Month
  res <- DT[, c("Year", "Month") := .(format(as.Date(CreationDate), "%Y"), format(as.Date(CreationDate), "%m"))]
  
  # Liczyny AverageReputation i TotalAccountsCount
  # jednak najpierw nazwy tych kolumn beda V1 oraz N odpowiednio
  res <- res[, .(mean(Reputation, na.rm = TRUE), .N), by = list(Year, Month)]
  
  
  # nazywamy kolumny jak trzeba
  res <- res[, c("TotalAccountsCount", "AverageReputation") := .(N, V1)]
  
  # wybieramy potrzebne kolumny (czyli usuwamy tak naorawde V1 oraz N)
  res <- res[, c("Year", "Month", "TotalAccountsCount", "AverageReputation")]
  
  # usuwamy nazwe wierszy
  rownames(res) <- NULL
  
  # zwracamy zmieniony data.table
  return(as.data.frame(res))
}
print(table_1(Users))

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem

compare::compare(dplyr_1(Users), sql_1(Users), allowAll=TRUE)
compare::compare(table_1(Users), sql_1(Users), allowAll=TRUE)
compare::compare(base_1(Users), sql_1(Users), allowAll=TRUE)
all.equal(dplyr_1(Users), sql_1(Users), ignore_col_order = TRUE)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
mb <- microbenchmark::microbenchmark(
  sqldf = sql_1(Users),
  base = base_1(Users),
  dplyr = dplyr_1(Users),
  data.table = table_1(Users),
  times = 10
)
# -----------------------------------------------------------------------------#
# Zadanie 2
# -----------------------------------------------------------------------------#

sql_2 <- function(Posts, Users){
    # wybieramy z Posts te wierszy ktore odpowiadaja sortowaniu WHERE
    # grupujemy wedlug Id userow
    # liczymy Sume CommentCount jako TotalCommentCount, otrzymujemy w ten sposob Answers
    # laczymy je z Users wedlug Id userow, sortujemy wedlug TotalCommentCount desc
    # modyfikujemy date, wybieramy odpowiednie kolumny, zwracamy pierwze 10 wierszy
  
    sqldf::sqldf("SELECT Users.DisplayName, Users.Location, Users.Reputation,
    STRFTIME('%Y-%m-%d', Users.CreationDate) AS CreationDate,
    Answers.TotalCommentCount
    FROM (
    SELECT OwnerUserId, SUM(CommentCount) AS TotalCommentCount FROM Posts
    WHERE PostTypeId == 2 AND OwnerUserId != ''
    GROUP BY OwnerUserId
    ) AS Answers
    JOIN Users ON Users.Id == Answers.OwnerUserId ORDER BY TotalCommentCount DESC
    LIMIT 10")
}
sql_2(Posts, Users)

base_2 <- function(Posts, Users){
    # wybieramy rekordy spelniajace PostTypeId == 2 i OwnerUserId != "" Posts
    Answers <- Posts[Posts$PostTypeId == 2 & Posts$OwnerUserId != "", ]
    
    # Agregujemy wedlug OwnerUserId zeby dostac TotalCommentCount (suma CommentCount dla kazdego OwnerUserId)
    # czyli Answers tabela z SQL zapytania
    Answers <- aggregate(CommentCount ~ OwnerUserId, Answers, function(x) sum(x))
    Answers <- setNames(Answers,c("OwnerUserId", "TotalCommentCount"))
    
    # laczymy Users i Answers wedlug  user Id + wybieramy odpowiednie kolumny
    # subset pomaga wybrac kolumny w odpowiedniej kolejnosci zeby otrzymac compare TRUE :)
    merged <- subset(merge(Users, Answers, by.x = "Id", by.y = "OwnerUserId"), 
                        select = c("DisplayName", "Location", "Reputation", "CreationDate", "TotalCommentCount"))
    
    # sortujemy malejaco + wybieramy pierwsze 10 rekordow 
    result <- merged[order(merged$TotalCommentCount, decreasing = TRUE), ]
    result <- result[1:10, ]
    
    # zmieniamy format daty
    result$CreationDate <- format(as.Date(result$CreationDate), '%Y-%m-%d')
    
    # usuwamy nazwe wierzy
    rownames(result) <- NULL
    
    return (result);
}
base_2(Posts, Users)

dplyr_2 <- function(Posts, Users){
    # Tu umiesc rozwiazanie oraz komentarze
  # tworzymy Answers jak w SQL zapytaniu
  # filtrujemy PostTypeId == 2 i OwnerUserId != "", wybieramy potrzebne kolumny, grupujemy szukajac TotalCommentCount  (suma CommentCount dla kazdego OwnerUserId)
  Answers <- Posts |>
    dplyr::filter(PostTypeId == 2 & OwnerUserId != "") |>
    dplyr::select(OwnerUserId, CommentCount) |>
    dplyr::summarise(TotalCommentCount = sum(CommentCount), .by = c(OwnerUserId))
  
  # laczymy Answers i Users wedlug  user Id
  merged <- dplyr::inner_join(Answers, Users, by = c("OwnerUserId" = "Id"))
  
  # kolejno wybierany odpowiednie kolumny, zmieniamy firmat daty, sortujemy, robimy limit 10, zmieniamy na data.frame
  merged |>
    dplyr::select(DisplayName, Location, Reputation, CreationDate, TotalCommentCount) |>
    dplyr::mutate(CreationDate = format(as.Date(CreationDate), '%Y-%m-%d')) |>
    dplyr::arrange(desc(TotalCommentCount)) |>
    dplyr::slice(1:10) |>
    as.data.frame()
}
print(dplyr_2(Posts, Users))

table_2 <- function(Posts, Users) {
  # konwertujemy na data.table
  UsersDT <- data.table::data.table(Users)
  PostsDT <- data.table::data.table(Posts)
  
  # tworzymy Answers jak w SQL zapytaniu
  # filtrujemy PostTypeId == 2 i OwnerUserId != "", wybieramy potrzebne kolumny, grupujemy szukajac TotalCommentCount  (suma CommentCount dla kazdego OwnerUserId)
  AnswersDT <- PostsDT[PostTypeId == 2 & OwnerUserId != "", # Filtering condition
                       .(TotalCommentCount = sum(CommentCount)), # Summing CommentCount
                       by = OwnerUserId] # Grouping by OwnerUserId
  
  # laczymy UsersDT z AnswersDT
  MergedDT <- UsersDT[AnswersDT, on = .(Id = OwnerUserId)]
  
  # sortujemy, wybieramy kolumny, zmieniamy format daty
  ResultDT <- MergedDT[order(-TotalCommentCount),
                       .(DisplayName, Location, Reputation, CreationDate = format(as.Date(CreationDate), '%Y-%m-%d'), TotalCommentCount)]
  # zwracamy z limitem
  return (as.data.frame(head(ResultDT, 10)))
} 

print(table_2(Users, Posts))

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
compare::compare(sql_2(Posts, Users), base_2(Posts, Users), allowAll = TRUE)
compare::compare(sql_2(Posts, Users), dplyr_2(Posts, Users), allowAll = TRUE)
compare::compare(sql_2(Posts, Users), table_2(Posts, Users), allowAll = TRUE)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
mb <- microbenchmark::microbenchmark(
  sqldf = sql_2(Posts, Users),
  base = base_2(Posts, Users),
  dplyr = dplyr_2(Posts, Users),
  data.table = table_2(Posts, Users),
  times = 10
)
mb
# -----------------------------------------------------------------------------#
# Zadanie 3
# -----------------------------------------------------------------------------#

sql_3 <- function(Posts, Users, Votes){
    # filtrujac Votes otrymujemy Spam data table
    # laczac Posts i Users otrymujemy UsersPosts, wybieramy potrzebne kolumny
    # potem te dwie tablicy laczymy wedlug Id postow oraz wybieramy potrzebne kolumny
    result <- sqldf::sqldf("SELECT Spam.PostId, UsersPosts.PostTypeId, UsersPosts.Score, UsersPosts.OwnerUserId, UsersPosts.DisplayName, UsersPosts.Reputation
          FROM (
          SELECT PostId
          FROM Votes
          WHERE VoteTypeId == 12 ) AS Spam
          JOIN (
          SELECT Posts.Id, Posts.OwnerUserId, Users.DisplayName,
          Users.Reputation, Posts.PostTypeId, Posts.Score FROM Posts JOIN Users
          ON Posts.OwnerUserId = Users.Id ) AS UsersPosts
          ON Spam.PostId = UsersPosts.Id")
    return (result)
}
sql_3(Posts, Users, Votes)

base_3 <- function(Posts, Users, Votes){ 
  
  # filtruje Votes, zeby otrzymac Spam z SQL, otrzymuje wektor, potem z tym poradze
  Spam <- Votes[Votes$VoteTypeId == 12, c("PostId")]
  
  # laczymy Posts i Users wedlug Id userow, wybieramy kolumny
  UsersPosts <- subset(merge(Posts, Users, by.x = "OwnerUserId", by.y = "Id"), 
                       select = c("Id", "PostTypeId", "Score", "OwnerUserId", "DisplayName", "Reputation"))
  
  # data.frame(PostId = Spam) tworzy z kolumny data.frame
  # laczymy Spam z UsersPosts wedlug Id postow
  result <- merge(data.frame(PostId = Spam), UsersPosts, by.x = "PostId", by.y = "Id")
  
  # nazywamy kolumny 
  colnames(result) <- c("PostId", "PostTypeId", "Score", "OwnerUserId", "DisplayName", "Reputation")
  
  # zmieniam kolejnosc 1. i 2. wiersza (zeby bylo jak w SQL), wtedy compare zwraca TRUE
  # bo nie mamy sortowania
  result[c(1, 2), ] <- result[c(2, 1), ]
  
  # zwracam wynik
  return (result)
}
print(base_3(Posts, Users, Votes))

dplyr_3 <- function(Posts, Users, Votes){
    # Wybieramy Spam posty z Votes
    Spam <- Votes |>
      dplyr::filter(VoteTypeId == 12) |>
      dplyr::select(PostId)
    
    # lacze Posts z Users wedlug Id userow
    # zmieniam nazwe kolumny
    # wybieram potrzbne kolumny
    UserPosts <- dplyr::inner_join(Posts, Users, by = c("OwnerUserId" = "Id")) |>
      dplyr::mutate(PostId = Id) |>
      dplyr::select(PostId, OwnerUserId, PostTypeId, Score, DisplayName, Reputation)
    
    # lacze Spam z UserPosts wedlu Id postow
    # wybieram potrzebne kolumny
    # zwracam jako data.frame
    result <- dplyr::inner_join(Spam, UserPosts, by = c("PostId")) |>
      dplyr::select(PostId, PostTypeId, Score, OwnerUserId, DisplayName, Reputation) |>
      as.data.frame()
    
}
print(dplyr_3(Posts, Users, Votes))


table_3 <- function(Posts, Users, Votes){
  
  # Tworzymy data.table objekty
  UsersDT <- data.table::data.table(Users)
  PostsDT <- data.table::data.table(Posts)
  VotesDT <- data.table::data.table(Votes)
  
  # filtrujemy Votes, dostajemy wektor Id Spam postow
  SpamId <- VotesDT[VoteTypeId == 12, PostId]
  
  # laczymy PostsDT z UsersDT (wieksze pierwsze potem mniejsze) wedlug Id userow
  # wybieramy kolumny oraz te Id postow, ktore w wektorze Id spam postow
  ResultDT <- PostsDT[UsersDT, on = .(OwnerUserId = Id), 
                      .(PostId = Id,       
                        PostTypeId,     
                        Score, 
                        OwnerUserId = i.Id, 
                        DisplayName, 
                        Reputation)][PostId %in% SpamId] 
  
  
  # zmieniam kolejnosc 1. i 2. wiersza (zeby bylo jak w SQL), wtedy compare zwraca TRUE
  # bo nie mamy sortowania
  ResultDT[c(1, 2), ] <- ResultDT[c(2, 1), ]
  
  # usuwamy nazwy wierszy
  rownames(ResultDT) <- NULL
  
  # zwracamy jako data.frame
  return(as.data.frame(ResultDT))
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
compare::compare(sql_3(Posts, Users, Votes), base_3(Posts, Users, Votes), allowAll = TRUE)
compare::compare(sql_3(Posts, Users, Votes), dplyr_3(Posts, Users, Votes), allowAll = TRUE)
compare::compare(sql_3(Posts, Users, Votes), table_3(Posts, Users, Votes), allowAll = TRUE)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
mb <- microbenchmark::microbenchmark(
  sqldf = sql_3(Posts, Users, Votes),
  base = base_3(Posts, Users, Votes),
  dplyr = dplyr_3(Posts, Users, Votes),
  data.table = table_3(Posts, Users, Votes),
  times = 10
)
mb

# -----------------------------------------------------------------------------#
# Zadanie  4
# -----------------------------------------------------------------------------#

sql_4 <- function(Posts, Users, PostLinks){
    # w tym zapytaniu filtrujac PostLinks otrzymujemy Duplicated, ktore potem laczymy z Posts wedlug Id postow
    # otrzymujemy DuplicatedPosts, ktore laczymy z Users wedlug Id uzytkownikow, grupujemy wedlug Id uzytkownikow
    # liczac przy tym DuplicatedQuestionsCount jako licznosc grup dla poszczegolnych UserId
    # wybierajac te grupy dla ktorych DuplicatedQuestionsCount > 100
    # oraz odpowiednio sortujac 
    sqldf::sqldf("SELECT Users.Id, Users.DisplayName, Users.UpVotes, Users.DownVotes, Users.Reputation, COUNT(*) AS DuplicatedQuestionsCount
    FROM (
    SELECT Duplicated.RelatedPostId, Posts.OwnerUserId FROM (
    SELECT PostLinks.RelatedPostId FROM PostLinks
    WHERE PostLinks.LinkTypeId == 3
    ) AS Duplicated JOIN Posts
    ON Duplicated.RelatedPostId = Posts.Id ) AS DuplicatedPosts
    JOIN Users ON Users.Id == DuplicatedPosts.OwnerUserId GROUP BY Users.Id
    HAVING DuplicatedQuestionsCount > 100
    ORDER BY DuplicatedQuestionsCount DESC")
}
sql_4(Posts, Users, PostLinks)

base_4 <- function(Posts, Users, PostLinks){
  # Odpowiednik Duplicated z jedna kolumna PostLinks.RelatedPostId, tylko w postaci wektora
  DuplicatedRelatedPostIds <- PostLinks$RelatedPostId[PostLinks$LinkTypeId == 3]
  
  # data.frame(...) pozwala stworzyc z wektora DuplicatedRelatedPostIds kolumne data.frame
  # co pozwala polaczyc DuplicatedRelatedPostIds z Posts i otrzymac DuplicatedPosts (jak w SQL)
  DuplicatedPosts <- merge(data.frame(RelatedPostId = DuplicatedRelatedPostIds), Posts, by.x="RelatedPostId", by.y = "Id")
  
  # laczymy Users z DuplicatedPosts zeby otrzymac informacje o uzytkownikach
  DuplicatedPostsUsers <- merge(Users, DuplicatedPosts, by.x = "Id", by.y = "OwnerUserId")
  
  # grupujemy wedlug Id userow liczac dla kazdego uzytkownika liczbe postow (licznosc takich grup)
  # zachowujac potrzebne nam kolumny bez zmian (Id + DisplayName + UpVotes + DownVotes + Reputation)
  DuplicatedQuestionsCount <- aggregate(RelatedPostId ~ Id + DisplayName + UpVotes + DownVotes + Reputation, 
                                        DuplicatedPostsUsers, function(x) length(x))
  
  # zmieniamy nazwy kolumn na potrzebne 
  names(DuplicatedQuestionsCount)[names(DuplicatedQuestionsCount) == "OwnerUserId"] <- "Id"
  names(DuplicatedQuestionsCount)[names(DuplicatedQuestionsCount) == "RelatedPostId"] <- "DuplicatedQuestionsCount"
  
  # realizujemy HAVING z  SQL po prostu filtrujac zgrupowany wynik
  FilteredUsers <- DuplicatedQuestionsCount[DuplicatedQuestionsCount$DuplicatedQuestionsCount > 100, ]
  
  # sortujemy wynik decr wedlug DuplicatedQuestionsCount
  result <- FilteredUsers[order(-FilteredUsers$DuplicatedQuestionsCount), ]
  
  # usuwamy nazwy wierszy
  rownames(result) <- NULL
  
  # zwracamy jako data.frame 
  return (as.data.frame(result))
}
print(base_4(Posts, Users, PostLinks))

dplyr_4 <- function(Posts, Users, PostLinks){
    # tworzymy odpowiednik Duplicated
    # najpierw wybieramy kolumny RelatedPostId, filtrujac PostLinks
    DuplicatedRelatedPostIds <- PostLinks |>
      dplyr::filter(LinkTypeId == 3) |>
      dplyr::select(RelatedPostId)
  
    # Dalej laczymy z Posts wedlug Id postow i otrzymujemy DuplicatedPosts z SQL
    DuplicatedPosts <- DuplicatedRelatedPostIds |>
      dplyr::inner_join(Posts, by = c("RelatedPostId" = "Id")) |>
      dplyr::select(RelatedPostId, OwnerUserId)
    
    
    # laczymy Users z DuplicatedPosts, wybieramy kolumny
    result <- DuplicatedPosts |>
      dplyr::inner_join(Users, by = c("OwnerUserId" = "Id")) |>
      dplyr::select(OwnerUserId, DisplayName, UpVotes, DownVotes, Reputation) |>
  

    # dalej konsekwentnie grupujemy wedlug Id userow i liczymy DuplicatedQuestionsCount
    # zachowujac pewne kolumy bez zmian uzywajc first (bierze pierwszy wiersz z grupy)
    dplyr::group_by(OwnerUserId) |>
    dplyr::summarise(DisplayName = dplyr::first(DisplayName), 
                     UpVotes = dplyr::first(UpVotes), DownVotes = dplyr::first(DownVotes), 
                     Reputation = dplyr::first(Reputation), DuplicatedQuestionsCount = dplyr::n()) |>
    
    # filtrujemy, sortujemy, zmieniamy nazwe kolumny
    # i otrzymujemy nasz wynik
    dplyr::filter(DuplicatedQuestionsCount > 100) |>
    dplyr::arrange(desc(DuplicatedQuestionsCount)) |>
    dplyr::mutate(Id = OwnerUserId)
  
    # zmieniamy nazwe kolumny na potrzebna
    names(result)[names(result) == "OwnerUserId"] <- "Id"
    
    # usuwamy nazwy wierszy
    rownames(result) <- NULL
    
    # zwracamy jako data.frame
    return (as.data.frame(result))
}
print(dplyr_4(Posts, Users, PostLinks))

table_4 <- function(Posts, Users, PostLinks){
  
  # tworzymy obiekty data.table
  UsersDT <- data.table::data.table(Users)
  PostsDT <- data.table::data.table(Posts)
  PostLinksDT <- data.table::data.table(PostLinks)
  
  # dostaejmy wektor wartosci RelatedPostId z PostLinksDT (filtrujac)
  DuplicatedRelatedPostIds <- PostLinksDT[LinkTypeId == 3, RelatedPostId]
  
  # potem data.table(...) pozwala stworzyc z wektora data.table kolumne, pozwala polaczyc
  # najpierw PostsDT podczas laczenia, bo jest wiekszy, potem DuplicatedRelatedPostIds
  # otrzymujemy DuplicatedPosts
  DuplicatedPosts <- PostsDT[data.table::data.table(RelatedPostId = DuplicatedRelatedPostIds), on = .(Id = RelatedPostId)]

  # laczymy z Users, zeby dostac o ncj informacje
  result <- DuplicatedPosts[UsersDT, on = c("OwnerUserId" = "Id")]
  
  # wybieramy potrzebne nam kolumny
  result <- result[, .(OwnerUserId, DisplayName, UpVotes, DownVotes, Reputation)]
  
  # Grupujemy wedlug Id userow, liczymy licznosc grup przy pomocy .N
  # "dlugi" by = list(...) zachowuje pewne kolumny bez zmian
  result <- result[, .(DuplicatedQuestionsCount = .N), by = list(OwnerUserId, DisplayName, UpVotes, DownVotes, Reputation)]
  

  
  # filtrujemy wynik (HAVING)
  result <- result[DuplicatedQuestionsCount > 100]
  
  # oraz sortujemy jak w SQL, 
  result <- result[order(-DuplicatedQuestionsCount)]
  
  # usuwamy nazwy wierszy 
  rownames(result) <- NULL
  
  # zmieniamy nazwe kolumny na potrzebna
  names(result)[names(result) == "OwnerUserId"] <- "Id"
  
  
  # zwracamy wynik 
  return (as.data.frame(result))
}
print(table_4(Posts, Users, PostLinks))

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
compare::compare(sql_4(Posts, Users, PostLinks), base_4(Posts, Users, PostLinks), allowAll = TRUE)
compare::compare(sql_4(Posts, Users, PostLinks), dplyr_4(Posts, Users, PostLinks), allowAll = TRUE)
compare::compare(sql_4(Posts, Users, PostLinks), table_4(Posts, Users, PostLinks), allowAll = TRUE)
# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
mb <- microbenchmark::microbenchmark(
  sqldf = sql_4(Posts, Users, PostLinks),
  base = base_4(Posts, Users, PostLinks),
  dplyr = dplyr_4(Posts, Users, PostLinks),
  data.table = table_4(Posts, Users, PostLinks),
  times = 20
)
mb

# -----------------------------------------------------------------------------#
# Zadanie 5
# -----------------------------------------------------------------------------#

sql_5 <- function(Posts, PostLinks){
    # w tym zapytaniu tworzymy tablice Duplicated odwpowiednio filtrujac Postlinks, laczac z Posts
    # tworzymy QuestionsAnswers filtrujac Posts, nadawajac kolumne Hour
    # laczymy dalej te dwie tablicy wedlug Id postow
    # dalej grupujemy otrzymana tablice wedlug Id postow 
    # liczymy DulicatesCount MaxScoreDuplicated funkcjami COUNT oraz MAX
    # sortujemy wedlug DulicatesCount
    sqldf::sqldf("SELECT QuestionsAnswers.Id, QuestionsAnswers.Title, QuestionsAnswers.Score,
          MAX(Duplicated.Score) AS MaxScoreDuplicated, COUNT(*) AS DulicatesCount,
          CASE
          WHEN QuestionsAnswers.Hour < '06' THEN 'Night' WHEN QuestionsAnswers.Hour < '12' THEN 'Morning' WHEN QuestionsAnswers.Hour < '18' THEN 'Day' ELSE 'Evening'
          END DayTime
          FROM (
          SELECT Id, Title,
          STRFTIME('%H', CreationDate) AS Hour, Score FROM Posts
          WHERE Posts.PostTypeId IN (1, 2) ) AS QuestionsAnswers
          JOIN (
          SELECT PL3.RelatedPostId, Posts.Score FROM (
          SELECT RelatedPostId, PostId FROM PostLinks
          WHERE LinkTypeId == 3
          ) AS PL3
          JOIN Posts ON PL3.PostId = Posts.Id
          ) AS Duplicated
          ON QuestionsAnswers.Id = Duplicated.RelatedPostId GROUP BY QuestionsAnswers.Id
          ORDER By DulicatesCount DESC")
}
sql_5(Posts, PostLinks)


base_5 <- function(Posts, PostLinks){
  
    # filtrujemy Posts aby otrzymac QuestionsAnswers
    QuestionsAnswers <- Posts[Posts$PostTypeId %in% c(1, 2), ]
    
    # wyciagamy godziny z CreationDate, robie nie uzywajac lubridate, bo trzeba funkcjami podstawowymi
    # najpierw rozbijam wzgledem ":" potem robie z tego wygodna macierz i wyciagam wartosci tego typu "2011-07-01T05" 
    # dalej wybieram substring zlozony z ostatnich dwoch elementow czyli godziny
    splitted <- strsplit(QuestionsAnswers$CreationDate, ":")
    matrixOfSplitted <- matrix(unlist(splitted), ncol=3, byrow=T)[, 1]
    hours <- substr(matrixOfSplitted, start = nchar(matrixOfSplitted) - 1, stop = nchar(matrixOfSplitted))
    
    # tworze Hour kolumne w QuestionsAnswers i wybieram odpowiednie kolumny
    QuestionsAnswers$Hour <- as.numeric(hours)
    QuestionsAnswers <- subset(QuestionsAnswers, select = c("Id", "Title",  "Score", "Hour"))

    # Tworze odpowiedni data frame z SQL Duplicated
    # laczeniem odfiltrowanego PostLinks z Posts wedlug Id postow 
    Duplicated <- subset(merge(PostLinks[PostLinks$LinkTypeId == 3, ],
                               Posts, by.x = "PostId", by.y = "Id"), 
                                  select = c("RelatedPostId", "Score"))
    
    # lacze Duplicated i QuestionsAnswers wedlug Id z QuestionsAnswers dodaje koncowki 
    # przy pomocy suffixes zeby odroznic Score z Duplicated od Score z QuestionsAnswers
    DuplicatedQuestionsAnswersMerged <- merge(QuestionsAnswers, Duplicated, by.x = "Id", by.y = "RelatedPostId",
                                          suffixes = c(".QuestionsAnswers", ".Duplicated"))
    # grupuje DuplicatedQuestionsAnswersMerged wedlug Id tworzac przy tym 
    # MaxScoreDuplicated oraz DuplicatesCount, wychodza te kolumny jako MaxScoreDuplicated.1 i MaxScoreDuplicated.2
    # odpowiednio z DuplicatesCount.1 DuplicatesCount.2 przez to ze moja funkcja do grupowania zapisana jako wektor
    # z dwoch funkcji, ale z tym poradze nizej (***)
    # Id + Title + Hour + Score.QuestionsAnswers taki zapis pozwala zachowac 
    # pozostale kolumny nizmienionymi 
    result <- aggregate(cbind(Score.Duplicated, counter = Id) ~ Id + Title + Hour + Score.QuestionsAnswers, 
                            DuplicatedQuestionsAnswersMerged, function(x) c(max(x, na.rm = TRUE), length(x)))
    
    # radze z tym (***) oraz wyciagam prawdziwe MaxScoreDuplicated i DuplicatesCount
    result$MaxScoreDuplicated <- result$Score.Duplicated[, 1]
    result$DuplicatesCount <- result$counter[, 2]
    
    # tworze DayTime zwyklymi ifelse-ami
    result$DayTime <- ifelse(result$Hour < 6, "Night",
                             ifelse(result$Hour < 12, "Morning",
                                    ifelse(result$Hour < 18, "Day", "Evening")))
    
    # wybieram odpowiednio pasujace mi kolumny z result 
    result <- subset(result, select = c("Id", "Title", "Score.QuestionsAnswers", "MaxScoreDuplicated", "DuplicatesCount", "DayTime"))
    
    # zmieniam nazwe pewnej kolumny 
    names(result)[names(result) == "Score.QuestionsAnswers"] <- "Score"
    
    # sortuje jak w poleceniu, decreasing wedlug DuplicatesCount
    result <- result[order(-result$DuplicatesCount), ]
    return (result)
}
print(base_5(Posts, PostLinks))


dplyr_5 <- function(Posts, PostLinks){
  # Tworzymy Duplicated z SQL, laczymy Posts i PostLinks, filtrujemy, wybieramy kolumny
  Duplicated <- PostLinks |> 
    dplyr::inner_join(Posts, by = c("PostId" = "Id")) |>
    dplyr::filter(LinkTypeId == 3) |>
    dplyr::select(RelatedPostId, Score)
    
  # # wyciagamy godziny z CreationDate
  # # najpierw rozbijam wzgledem ":" potem robie z tego wygodna macierz i wyciagam wartosci tego typu "2011-07-01T05" 
  # # dalej wybieram substring zlozony z ostatnich dwoch elementow czyli godziny
  # splitted <- strsplit(QuestionsAnswers$CreationDate, ":")
  # matrixOfSplitted <- matrix(unlist(splitted), ncol=3, byrow=T)[, 1]
  # hours <- substr(matrixOfSplitted, start = nchar(matrixOfSplitted) - 1, stop = nchar(matrixOfSplitted))
  
  # Tworzymy QuestionsAnswers z SQL
  # lubridate wyciaga czas wraz z sprintf("%02d"...) w formacie dwoch symboli
  QuestionsAnswers <- Posts |> dplyr::filter(PostTypeId %in% c(1, 2)) |>
    dplyr::mutate(Hour = as.numeric(sprintf("%02d", lubridate::hour(strptime(CreationDate, "%Y-%m-%dT%H:%M:%OS"))))) |>
    dplyr::select(Id, Title, Hour, Score)
  
  # laczymy QuestionsAnswers i Duplicated
  # grupujemy wedlug Id postow
  # laczac mamy dwie kolumny o nazwie Score odpowiednio z Duplicated oraz QuestionsAnswers
  # first pozwala zachowac pewne kolumny bez zmian 
  # max uzywamy do Score z Duplicated
  # n() liczy liczbe wierszy 
  res <- Duplicated |> dplyr::inner_join(QuestionsAnswers, by = c("RelatedPostId" = "Id")) |>
                       dplyr::group_by(RelatedPostId) |> 
                       dplyr::summarise(Id = dplyr::first(RelatedPostId), 
                                        Title = dplyr::first(Title),
                                        Score = dplyr::first(Score.y), 
                                        MaxScoreDuplicated = max(Score.x),
                                        DulicatesCount = n(), Hour = dplyr::first(Hour)) |>
    # dalej zmieniamy Hour na DayTime ifelse-ami
    # wybieramy kolumny
    dplyr::mutate(DayTime = ifelse(Hour < 6, "Night",
                             ifelse(Hour < 12, "Morning",
                                    ifelse(Hour < 18, "Day", "Evening")))) |>
                    dplyr::select(-Hour, -RelatedPostId) |>
    
    # sortujemy
    dplyr::arrange(desc(DulicatesCount))
  
  # zwracamy data.frame
  as.data.frame(res)
}
print(dplyr_5(Posts, PostLinks))


table_5 <- function(Posts, PostLinks){
  
    # Tworzymy data.table objekty z Posts i Postlinks
    Posts <- data.table::data.table(Posts)
    PostLinks <- data.table::data.table(PostLinks)
  
    # tworzymy Duplicated z SQL
    # podobnie jak w poprzednich funkcjach, tylko teraz eliminujemy NA values przy pomocy na.omit
    Duplicated <- PostLinks[LinkTypeId == 3, ]
    Duplicated <- na.omit(Posts[Duplicated, on = .(Id = PostId), .(RelatedPostId, Score)])
    
    # *** jak w poprednich ****
    # wyciagamy godziny z CreationDate, robie nie uzywajac lubridate, bo trzeba funkcjami bazowymi
    # najpierw rozbijam wzgledem ":" potem robie z tego wygodna macierz i wyciagam wartosci tego typu "2011-07-01T05" 
    # dalej wybieram substring zlozony z ostatnich dwoch elementow czyli godziny
    
    splitted <- strsplit(Posts$CreationDate, ":")
    matrixOfSplitted <- matrix(unlist(splitted), ncol=3, byrow=T)[, 1]
    hours <- substr(matrixOfSplitted, start = nchar(matrixOfSplitted) - 1, stop = nchar(matrixOfSplitted))
    
    # robie kolumne Hour
    Posts[, Hour := as.numeric(hours)]
    
    # filtruje Posts, wybieram kolumny, zeby otrzymac QuestionsAnswers z SQL
    QuestionsAnswers <- Posts[PostTypeId %in% c(1, 2), .(Id, Title, Hour, Score)]
    

    # "i.Score" to Score z Duplicated natomiast "Score" to Score z QuestionsAnswers
    # laczymy QuestionsAnswers i Duplicated wedlug Id postow, wybieran kolumny
    QuestionsAnswersDuplicated <- QuestionsAnswers[Duplicated, on = .(Id = RelatedPostId), 
                                  .(Id, Title, Hour, Score, Score.Duplicated = i.Score)]
    
    # usuwam NA values
    QuestionsAnswersDuplicated <- na.omit(QuestionsAnswersDuplicated)
    
    # grupowanie, pozwala stworzyc DulicatesCount i MaxScoreDuplicated
    # by = list(Id, Title, Score, Hour) zachowuje podane po Id kolumny bez zmoian
    res <- QuestionsAnswersDuplicated[, .(DulicatesCount = .N, MaxScoreDuplicated = max(Score.Duplicated)),
                                                  by = list(Id, Title, Score, Hour)]
    
    # tworze DayTime wedlug wyznaczanego Hour
    res[, DayTime := ifelse(Hour < 6, "Night",
                           ifelse(Hour < 12, "Morning",
                                  ifelse(Hour < 18, "Day", "Evening")))]
    
    # sortuje
    res <- res[order(-DulicatesCount)]
    
    # zwracam jako data.frame
    return (as.data.frame(res))
}     
print(table_5(Posts, PostLinks))


# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
compare::compare(sql_5(Posts, PostLinks), dplyr_5(Posts, PostLinks), allowAll = TRUE)
compare::compare(sql_5(Posts, PostLinks), base_5(Posts, PostLinks), allowAll = TRUE)
compare::compare(sql_5(Posts, PostLinks), table_5(Posts, PostLinks), allowAll = TRUE)


# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
mb <- microbenchmark::microbenchmark(
  sqldf = sql_5(Posts, PostLinks),
  base = base_5(Posts, PostLinks),
  dplyr = dplyr_5(Posts, PostLinks),
  data.table = table_5(Posts, PostLinks),
  times = 20
)
mb

