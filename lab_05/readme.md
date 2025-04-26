# Przetwarzanie dużych zbiorów danych

## Lab 5: Wstęp do Apache Spark.

### 1. Przygotowanie środowiska.

#### 1.1 Scenariusz 1 - lokalna instalacja Spark

**UWAGA!!!**

**Ten scenariusz został uruchomiony dla Pythona w wersji 3.11.9. Uruchomienie dla nowszej wersji nie działało poprawnie dla tej konfiguracji na maszynie prowadzącego!**

Spark jest natywnie dostępny dla systemów Linux, ale można go również wykorzystać w systemie Windows.

Istnieje kilka alternatywnych sposobów jego instalacji, tutaj zostanie opisany sposób polegający na instalacji modułu `pyspark`.

**Krok 1 - instalacja Java SDK**

Ze strony https://www.openlogic.com/openjdk-downloads pobieramy instalator Javy w wersji 17.

Na ekranie instalacji wybieramy opcję utworzenia zmiennej środowiskowej `JAVA_HOME`. Ta zostanie dodana do zmiennych środowiskowych systemu, co może nie być wystarczające i możliwe, że konieczne będzie dodanie zmiennej o takiej samej nazwie w przestrzeni zmiennych danego użytkownika.

Pamiętaj, że zmiana wartości zmiennych środowiskowych wymaga przeładowania terminala, jeżeli chcemy tam mieć do nich dostęp.

W terminalu możemy sprawdzić czy zmienna jest i zawiera odpowiedni wpis poleceniem `echo %java_home%` w systemie Windows lub `echo $JAVA_HOME` w systemach Unix (Linux, Mac).

**Krok 2 - stworzenie środowiska wirtualnego Python na potrzeby projektu.**

Tutaj w zależności od wykorzystywanego narzędzia na zajęciach sposób jego utworzenia może być różny i zostanie przedstawiony w trakcie spotkania. Procedura jest analogiczna do tej przedstawionej na poprzednich zajęciach związanych z językiem Python.

**Krok 3 - instalacja niezbędnych modułów do środowiska wirtualnego.**

Jeżeli nasze środowisko wirtualne nie jest aktywowane (np. wykorzystujemy terminal) to je aktywujemy, a następnie instalujemy:
```console
pip install pyspark jupyterlab findspark
```

Ten proces może chwilę zająć, gdyż paczki te wymagaja pobrania ~500 MB danych z repozytorium Pypi.

**Krok 4 - uruchomienie Jupyter Lab i test działania środowiska Spark**

Uruchmiamy Jupyter Lab poleceniem `jupyter lab` w terminalu i czekamy, aż otworzy się okno domyślnej przeglądarki.
Do folderu projektu kopiujemy Jupyter Notebook, który został dostarczony w tym labie. Postępujemy zgodnie z instrukcjami w nim zawartymi.

#### 1.2 Scenariusz 2 - lokalnie z wykorzystaniem narzędzia Docker


Środowisko pracy, które będzie wykorzystane na zajęciach składa się z lokalnego klastra Spark uruchamianego na kontenerze Linuksowym oraz wykorzystania API Pythona (PySpark) do komunikacji ze środowiskiem Spark.

**Krok 1**

Instalacja Dockera.

Najwygodniej będzie korzystać z `Docker Desktop`, którego instalujemy ze strony https://docs.docker.com/get-started/get-docker/.
W systemach Linux oraz Mac sam docker engine może być już zainstalowany, więc można to sprawdzić w terminalu poleceniem `docker --version`.

**Krok 1**  

Na dysku lokalnym utwórz folder na potrzeby zajęć. Zaleca się jego zachowanie pomiędzy zajęciami, gdyż będzie wielokrotnie wykorzystywany, więc najlepiej opakować to docelowo w repozytorium git.

W stworzonym folderze umieść plik o nazwie `Dockerfile` (bez rozszerzenia) o podanej poniżej zawartości.

```bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM spark:3.5.3-scala2.12-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /home/spark; \
	chown spark:spark /home/spark 

USER spark
```

**Krok 2**  

Otwórz terminal i przejdź do utworzonego wcześniej folderu. Wywołaj polecenie `docker build .`.

Więcej szczegółów tego polecenia znajdziesz w dokumentacji: https://docs.docker.com/reference/cli/docker/buildx/build/

Wykonanie tego polecenia będzie wymagało aktywnego połączenia z Internetem i może zająć sporo czasu w zależności od aktualnego obciążenia łącza. Do pobrania jest nieco ponad 1 GB danych.

Jeżeli proces budowania obrazu Docker zakończył się poprawnie sprawdź w aplikacji Docker Desktop czy obraz jest widoczny w zakładce `Images`.

Jeżeli nazwa obrazu powinna być zmieniona (np. z <none>) to możemy to wykonać poprzez polecenie poniżej:

```bash
# d583c3ac45fd trzeba zastąpić skrótem konkretnego obrazu, można
# to skopiować z aplikacji Docker Desktop
docker image tag d583c3ac45fd nowa_nazwa_obrazu:nowy_tag
```

**Krok 3**  

Z racji tego, że kontenery nie utrwalają żadnych zmian w systemie plików, które dokonały się w trakcie jego pracy pomiędzy kolejnymi uruchomieniami kontenera, potrzebujemy miejsca, w którym najważniejsze pliki z aplikacjami i danymi będziemy przechowywać w lokalnym systemie plików.

W tym samym folderze utwórz katalog o nazwie `apps`.

Aby możliwe było korzystanie z lokalnego folderu konieczna jest konfiguracja `volume` dla uruchamianego kontenera. Z racji tego, że będziemy również potrzebować mapowania portów z kontenera na host najłatwiejszym rozwiązaniem będzie stworzenie pliku `docker-compose.yml` i przechowanie konfiguracji właśnie w nim, co pozwoli nam na łatwą modyfikację (i dodanie kolejnych węzłów klastra) w przyszłości.

Utwórz plik `docker-compose.yml` w tym samym folderze co plik `Dockerfile` o zawartości jak poniżej (z modyfikacjami):

```yml
services:
  spark-master-3.5.3:
    image: spark-3.5.3:v3
    tty: true
    stdin_open: true
    command: bash
    ports:
      - "9090:8080"
      - "7077:7077"
      - "8888:8888"
      - "4040:4040"
    volumes:
       - ./apps:/opt/spark/work-dir
```

Linia `spark-master-3.5.3` oznacza nazwę kontenera, którą możesz zmienić na coś innego. Twój obraz też może mieć inną nazwę niż `spark-3.5.3:v3` więc dostosuj ją odpowiednio. Jeżeli mapowanie portów wymaga modyfikacji wprowadź ją. W ostatnich wierszach następuje mapowanie lokalnego folderu `apps` na folder `/opt/spark/work-dir`, które w kontenerze będzie już istniał po uruchomieniu.

Aby wykonać polecenia z powyższego pliku uruchom w terminalu polecenie `docker compose up`.

Sprawdź czy kontener został uruchomiony.

**Krok 4**  

Aby możliwe było zainstalowanie niezbędnych pakietów Pythona oraz ich przechowywanie między uruchomieniami kontenera stworzymy środowisko wirtualne wewnątrz folderu apps i tam będziemy instalować paczki Pythona.

Na początek skorzystamy z zakładki `Exec` w programie Docker Desktop na uruchomionym wcześniej kontenerze. Domyślnie prawdopodobnie zostanie uruchomiony shell `sh`, ale wpisując polecenie `bash` możemy to zmienić w tej sesji.

Jeżeli potrzebujemy kolejnego okna konsoli podłączonej do tego samego kontenera to możemy je uruchomić z poziomu terminala hosta w poniższy sposób:

```bash
docker [container] exec -it <container> <command>
# np. 
docker exec -it spark-3.5.3:v3 bash
```

Sprawdź czy dostępny jest moduł `virtualenv` dla zainstalowanego interpretera Python 3. Jeżeli nie to należy go zainstalować poprzez moduł `pip3`.

Zmodyfikowaliśmy teraz pliki wewnątrz kontenera, ale po jego restarcie modułu  `virtualenv` nie będzie w systemie w kontenerze. Jeżeli chcemy zachować jakieś zmiany w obrazie możemy wykonać commit i zapisać postać kontenera w nowym obrazie. Służy do tego polecenie `docker [container] commit`.

```bash
docker commit skrót-hasha-kontenera  nowa-nazwa-obrazu:tag
```

Teraz aby uruchamiać kontenery z nowej postaci obrazu należy zmodyfikować plik `docker-compose.yml` aktualizując nazwę obrazu i ponownie wywołać `docker compose up`.


**Krok 5**

Stwórz nowe środowisko wirtualne z poziomu kontenera tak, aby zostało umieszczone w zmapowanym folderze `apps` i było dostępne pomiędzy uruchomieniami kontenera.


### 2. Uruchomienie środowiska Jupyter Lab.


Zainstaluj pakiet `jupyterlab` do stworzonego wcześniej środowiska wirtualnego. Reszta niezbędnych paczek również będzie instalowana w tym środowisku. Pamiętaj o jego aktywacji przed wykonaniem instalacji.

Uruchomienie jupyterlab wymaga podania parametru `--ip`, w przeciwnym wypadku nie będzie można dostać się do niego z poziomu przeglądarki komputera hosta. Dodatkowo możemy wskazać, żeby nie szukać i nie otwierać domyślnej przeglądarki www w kontenerze.

```bash
python3 -m jupyterlab --ip 0.0.0.0 --no-browser
```

Uruchamiamy Jupyter Lab w przeglądarce na maszynie lokalnej z adresu http://localhost:8888 jeżeli ten port nie był już wcześniej zajęty.

W celu przyspieszenia wykonywania komend z tego punktu przy kolejnych uruchomieniach środowiska można stworzyć skrypt bash, który aktywuje środowisko i uruchomi Jupyter lab.

Przykładowa zawartość skryptu:
```bash
#!/usr/bin/bash
source /opt/spark/work-dir/.venv/bin/activate && python3 -m jupyterlab --ip 0.0.0.0 --no-browser
```

### 3. Przykłady i zadania.

Przykładowy kod + zadania do tego labu znajdują się w pliku [01_pyspark_introduction.ipynb](01_pyspark_introduction.ipynb), który należy umieścić w folderze współdzielonym między komputerem hosta a kontenerem i wczytać z poziomu Jupyter Lab.
