### 1. Repozytorium projektu z przedmiotu *Zaawansowane techniki baz danych*

#### 1.1. Temat i cel projektu
<div>
Tematem projektu jest porównanie dwóch systemów bazodanowych - relacyjnego 
(<a href="https://www.mysql.com/"><i>MySQL</i></a>) oraz 
nierelacyjnego (<a href="https://www.mongodb.com/"><i>MongoDB</i></a>), 
tak aby przetestować działanie obu systemów, wykorzystując 
duże zbiory danych. Projekt obejmuje również analizę otrzymanych wyników i 
wnioski w formie sprawozdania.
</div>

#### 1.2. Sprawozdanie
Efektem końcowym projektu jest sprawozdanie, które obejmuje:
- Cel i zakres pracy,
- Opis wybranych systemów bazodanowych,
- Zalety i wady wybranych baz (udogodnienia, ograniczenia)
- Cechy takie jak: awaryjność, bezpieczeństwo, migracje, integracje, skalowalność,
- Obszary zastosowania wybranych SZBD,
- Opis zbioru danych,
- Opis aplikacji do przeprowadzenia testów (zdefiniowanie wymagań,
wykorzystane technologie i narzędzia, opis działania)
- Opis przeprowadzonych testów wydajnościowych - porównanie działania przy operacjach CRUD dla małego, średniego i dużego zestawu danych, 
- Opracowanie wyników przeprowadzonych testów w postaci opisu oraz wykresów.

### 2. Przykładowe operacje - testy

<div> 
Poniżej wypisane są przykłady operacji bazodanych, które należy przetestować 
dla obu systemów.
</div>

#### 2.1. Założenia:
- Zbiór danych taki sam lub bardzo zbliżony (z racji, że systemu relacyjnego nie odwzorujemy 1:1 w bazie nierelacyjnej),
- Dane odpowiednio znormalizowane,
- Operacje przeprowadzane na równolicznych zbiorach danych (od zbioru małego, np. 1000 rekordów, do np. 1000000 rekordów),
- Schemat bazy danych wolny od anomalii,

#### 2.2. Ewentualne scenariusze testowe:
- Operacje wykorzystujące indeksowanie i te bez indeksów, 
- W przypadku bazy nierelacyjnej można przetestować dwa schematy powiązań (*ang. embedded, referencing*) 

#### 2.3. Operacje
Będziemy testować operacje CRUD-owe

**2.3.1. Create (*INSERT*)**
- **Bulk Insert** - INSERT rekordów w jednej operacji,
- **Single Insert** - pojedyncza operacja INSERT wykonywana w pętli.

*Uwagi*
1) Dla bazy relacyjnej można wykorzystać procedurę, która wykonywała by INSERTy do różnych tabel,
2) Może warto by było wykorzystać wątki* 

**2.3.2. Read (*SELECT*)**
- **Prosty SELECT** - pobieranie rekordów z tabeli z wykorzystaniem `id`,
- **Query z filtrowaniem** - pobieranie rekordów z tabeli z wykorzystaniem klauzuli `WHERE` 
- **Funkcje agregujące** - zapytania wykorzystujące `GROUP BY` oraz `aggregate`,
- **Zapytania z wielu tabel** - wykorzystanie metod `JOIN` oraz `$lookup`.

*Uwagi*
1. Porównanie działania dla danych indeksowanych oraz bez indeksów dla pól (np. `id`),
2. W przypadku bazy nierelacyjnej użycie `$lookup` możliwe jest tylko w przypadku podejścia *referencing*.

**2.3.3. Update**
Sytuacja podobna jak w przypadku Insert:
- **pojedyńczy Update** - aktualizacja jednego rekordu, lub wielu rekordów w pętli, w MySQL możemy wykorzystać procedurę, która wykonuje aktualizacje w kilku powiązanych tabelach, 
- **Bulk Update** - aktualizacja wielu rekordów, z warunkiem `WHERE` lub bez,
- **Update powiązanych rekordów** - aktualizacja danych w innej tabeli za pomocą klucza obcego.

**2.3.4. Delete**
Analogicznie jak Update:
- **usuwanie pojedynczych danych** - np. usunięcie użytkownika\użytkowników o danym `id`,
- **Bulk Delete** - usunięcie wielu rekordów - np. usunięcie wszystkich użytkowników o podanym `id_dorm`,

Dodatkowo:
- **Cascade Delete** - usunięcie rekordów i wszystkich powiązanych danych z innych tabel, wykorzystując klucz obcy; w przypadku MongoDB usuwanie powiązanych dokumentów.

*Uwagi*
Poniższe uwagi odnoszą się do wszystkich wymienionych wyżej operacji:

1. Dodatkowo można testować operacje mieszane, np. w jednej "partii" wykonujemy naraz INSERT, UPDATE oraz DELETE,
2. Wątki i transakcyjność - można zbadać jak system bazodanowy zachowuje się, kiedy operacje wykowywane są równolegle przez kilka wątków (np. dwa wątki próbują coś zaktualizować w jednej tabeli/dokumencie)*

### 3. Sprawozdanie - dane

#### 3.1. Dane do przeanalizowania (przykłady)
- Czas wykonania poszczególnych operacji,
- Ilość operacji wykonywanych np. w sekundę,
- Użycie zasobów systemowych,
- Skalowalność - jak system reaguje na zwiększanie danych do przetworzenia,
- Spójność danych po wykonaniu operacji,
- **Wielowątkowość* - w zależności od postępów.

#### 3.2. Kryteria porównawcze

1. Model danych 
- opis systemu relacyjnego i nierelacyjnego,
- wady i zalety każdego z nich,
- obszary zastosowań,
- ogólnie, cechy charakterystyczne, możliwości i ograniczenia poszczególnych SZBD,
- skalowalność systemu,
- transakcyjność.

2. Awaryjność i kopia zapasowa
- utraty danych, awarie i odzyskiwanie.

3. Wydajność systemu
- dane zgromadzone podczas testów, w postaci tabel i wykresów. 

### 4. Aplikacja
Aplikacja uruchamia się w terminalu, do uruchomienia potrzebny jest 
interperter języka Python (3.12+) oraz Docker. Aby uruchomić program należy wykonać poniższe komendy w podanej kolejności.
```console
docker-compose up -d
pip install -r requirements.txt
python main.py
```

### 5. Wnioski
W sprawozdaniu