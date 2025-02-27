## Protocolul BitTorrent

## Implementare

### Peer
Incepe prin a citi din fisierele de intrare ce fisiere detine si ce chunk-uri are fiecare (hash-urile).
Citeste apoi ce fisiere isi doreste, si marcheaza pe fiecare ca avand -1 chunk-uri.
Trimie apoi catre tracker o actualizare a chunk-urilor pe care le detine.
Porneste thread-urile de *download* si *upload*.

#### Download thread

Pentru fiecare fisier pe care il doreste, face initial un request catre tracker pentru 
a afla din ce chunk-uri este format fisierul respectiv (hash-urile in ordine).
Apoi face un request pentru afla ce peers detin chunk-uri din fisierul respectiv si incepe sa le 
descarce pe rand in ordinea aflata anterior. La fiecare 10 chunk-uri descarcate trimite o actualizare 
catre tracker cu ce chunk-uri detine. Comunicarea cu trackerul se face cu tag-ul DOWNLOAD_THREAD_TAG. 
Pentru a cere un chunk unui alt peer foloseste tag-ul UPLOAD_THREAD_TAG. Raspunsul vine cu tag-ul 
DOWNLOAD_THREAD_TAG, iar peer-ul stie ca este raspunsul la cererea lui datorita rank-ului sursa.
Cand termina de descarcat complet un fisier, scrie in fisierul de output hash-urile din owned_chunks.

#### Upload thread

Primeste mesaje cu tag-ul UPLOAD_THREAD_TAG. Cat timp nu primeste un mesaj de la tracker, inseamna
ca trebuie sa raspunda unui alt peer pentru a-i face "upload" cu chunk-ul dorit. Daca mesajul este 
de la tracker, thread-ul se opreste. Raspunde la cererile celorlalti peers cu tag-ul DOWNLOAD_THREAD_TAG.

### Tracker

Incepe prin a primi cate o actualizare a fisierelor detinute de la fiecare peer.
Isi construieste astfel si listele chunk-urilor pentru fiecare fisier din retea.

Cat timp mai are peers conectati (care descarca), primeste requesturi. Ele au 4 tipuri:
1. (req == 0) un peer doreste sa afle din ce chunk-uri este format un fisier pe care il doreste
2. (req == 1) un peer doreste sa afle cine are chunk-uri dintr-un fisier pe care il doreste
3. (req == 2) un peer a terminat de descarcat toate fisierele pe care le dorea
4. (req == 3) un peer a trimis o actualizare a chunk-urilor pe care le detine

Cand nu mai sunt peers conectati trimite cate un mesaj catre toti peers (cu tag-ul UPLOAD_THREAD_TAG) 
care ii anunta sa-si inchida thread-ul de upload.
