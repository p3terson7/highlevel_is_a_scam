Voici le résumé ciblé que je te conseillerais de maîtriser pour cet examen. Je l’ai orienté selon les chapitres 2 à 5 et surtout selon ce qui revient dans les exercices corrigés.

**Vue d’ensemble**
- `Application` : communication entre applications.
- `Transport` : communication `processus à processus`.
- `Réseau` : communication `hôte à hôte`, adressage IP, routage.
- `Liaison` : communication `entre nœuds adjacents`, trames, MAC, accès au média.
- Quand on descend les couches, on `encapsule` en ajoutant des entêtes. Quand on remonte, on `désencapsule`.

**1. Couche application**
- `Client/serveur` : le serveur est généralement toujours actif, a souvent une IP permanente, et les clients ne communiquent pas directement entre eux.
- `P2P` : les pairs peuvent communiquer directement; un pair peut jouer le rôle de client et de serveur.
- Un `socket` est l’interface entre `application` et `transport`.
- Pour identifier une communication, retiens : `IP source + IP destination + port source + port destination + protocole de transport`.

**Services demandés par les applications**
- `Fiabilité` : ex. transfert de fichier, courriel.
- `Débit` : ex. multimédia.
- `Délai` : ex. téléphonie IP, jeux interactifs.
- `Sécurité/intégrité` : pas assurée automatiquement par TCP dans ce cours.

**TCP vs UDP pour les applications**
- `TCP` : fiable, orienté connexion, contrôle de flux, contrôle de congestion.
- `UDP` : non fiable, sans connexion, pas de contrôle de flux, pas de contrôle de congestion, petit entête.
- Une application sur UDP peut quand même implémenter sa propre fiabilité au niveau application.

**HTTP**
- `HTTP` utilise `TCP`, port `80`.
- `HTTP est sans état`.
- `Non persistant` : 1 connexion TCP par objet.
- `Persistant` : plusieurs objets sur la même connexion TCP.
- Temps de réponse HTTP non persistant : `2 RTT + temps de transmission de l’objet`.
- Méthodes à connaître :
- `GET` : récupérer une ressource.
- `POST` : envoyer des données dans le corps.
- `HEAD` : récupérer les infos sans récupérer l’objet.
- `PUT` : téléverser.
- `DELETE` : supprimer.
- Codes à retenir :
- `200 OK`
- `301 Moved Permanently`
- `400 Bad Request`
- `404 Not Found`
- `505 HTTP Version Not Supported`

**FTP**
- Utilise `TCP`.
- `Connexion de contrôle` sur port `21`.
- `Connexion de données` séparée, classiquement port `20`.
- FTP est `avec état` : authentification, répertoire courant, etc.

**Courriel**
- `SMTP` : envoi de courriels, `TCP`, port `25`, protocole `push`.
- `POP3` : récupération simple des courriels, plutôt téléchargement, peu d’état.
- `IMAP` : garde les messages sur le serveur, gère dossiers et état.
- Si Alice envoie via navigateur web et Bob lit avec Outlook :
- Alice ↔ serveur mail : `HTTP`
- serveur mail Alice ↔ serveur mail Bob : `SMTP`
- Bob ↔ serveur mail Bob : `POP3` ou `IMAP`

**DNS**
- Associe `nom de domaine ↔ adresse IP`.
- DNS est une `base de données distribuée hiérarchique`.
- Serveurs à connaître :
- `local`
- `racine`
- `TLD`
- `autoritaire`
- Port `53`.
- Le cours montre surtout des requêtes DNS via `UDP`, mais mentionne aussi `TCP ou UDP`.
- Résolution typique : client → DNS local → racine → TLD → autoritaire.

**2. Couche transport**
- Rôle : communication logique `entre processus`.
- `Multiplexage/démultiplexage` : les ports servent à livrer le segment au bon socket.
- Retenir pour l’examen :
- `UDP` : socket vu par le cours comme un doublet.
- `TCP` : socket identifié par un `quadruplet`.

**UDP**
- `Best effort` : perte, désordre, duplication possibles.
- Entête UDP = `8 octets`.
- `Checksum` : détecte des erreurs, mais ne garantit pas à 100 % qu’il n’y en a pas.
- UDP ne garantit ni :
- ordre
- non-duplication
- livraison sans perte

**TCP**
- `Fiable`
- `orienté connexion`
- `flux d’octets ordonnés`
- `full duplex`
- Numéro de séquence = `numéro du premier octet du segment`.
- ACK = `prochain octet attendu`.
- `ACK cumulatif`
- Fiabilité assurée par :
- checksum
- ACK
- retransmission après `timeout`
- retransmission rapide après `3 ACK dupliqués`

**Ouverture/fermeture TCP**
- Ouverture : `3-way handshake`
- `SYN`
- `SYNACK`
- `ACK`
- Fermeture : échange avec `FIN` et `ACK`

**Contrôle de flux**
- Le récepteur annonce `rwnd`.
- L’émetteur ne doit pas avoir plus de données non acquittées que la fenêtre permise.

**Contrôle de congestion**
- `cwnd` = fenêtre de congestion.
- `slow start` : croissance exponentielle.
- `congestion avoidance` : croissance linéaire.
- Passage à `congestion avoidance` quand `cwnd` atteint `ssthresh`.
- Si perte par `timeout` : retour fort vers slow start.
- Si perte par `3 ACK dupliqués` :
- `Tahoe` : retour slow start
- `Reno` : réduction puis reprise en congestion avoidance

**3. Couche réseau**
- Internet est un réseau à `datagrammes`, pas à circuits virtuels.
- Dans un réseau à datagrammes :
- pas d’établissement de connexion réseau
- pas d’état de connexion dans les routeurs
- les paquets utilisent l’`adresse de destination`
- deux paquets entre les mêmes hôtes peuvent suivre des chemins différents
- Dans un `circuit virtuel` :
- établissement avant transfert
- chaque paquet porte un identifiant de circuit
- les routeurs gardent de l’état

**Datagramme IP**
- Entête IP minimale = `20 octets`
- Champs importants :
- `TTL` : décrémenté à chaque routeur; quand il vaut 0, le paquet est détruit
- `Protocol` : ex. `6 TCP`, `17 UDP`
- `DF` : ne pas fragmenter
- `MF` : plus de fragments à venir
- `offset` : position du fragment en multiples de `8 octets`

**Fragmentation IP**
Méthode à savoir faire :
- `données à fragmenter = taille totale - entête`
- `taille max données/fragment = floor((MTU - entête)/8) * 8`
- `offset = nombre d’octets de données déjà envoyés / 8`
- `MF = 1` sauf pour le dernier fragment
- `ID` reste le même pour tous les fragments
- Le réassemblage se fait à la `destination finale`

**Adressage IPv4**
- `Masque` : bits réseau à `1`, bits hôte à `0`
- Nombre d’hôtes utilisables si `n` bits hôte : `2^n - 2`
- Un routeur peut avoir plusieurs interfaces et donc plusieurs adresses IP.
- Un sous-réseau regroupe les interfaces qui ont la même `partie réseau/sous-réseau`, pas la même partie hôte.

**Classful + pièges**
- Classe `A` : `/8`
- Classe `B` : `/16`
- Classe `C` : `/24`
- Classe `D` : multicast, pas pour un hôte normal
- `127.0.0.1` : boucle locale
- `255.255.255.255` : diffusion générale
- Une adresse finissant par `.255` n’est pas toujours broadcast : ça dépend du `masque`.

**Adresses privées**
- `10.0.0.0/8`
- `172.16.0.0/12`
- `192.168.0.0/16`

**CIDR / VLSM**
Méthode très importante :
- compter tous les sous-réseaux, y compris les liaisons entre routeurs
- pour chaque sous-réseau : `interfaces nécessaires + adresse réseau + broadcast`
- choisir le plus petit bloc de taille puissance de 2 suffisant
- attribuer du `plus grand au plus petit`
- donner pour chaque sous-réseau :
- adresse réseau
- masque
- première adresse
- dernière adresse
- broadcast
- Sur une liaison routeur-routeur, les exercices utilisent souvent `4 adresses` au total, donc `/30`.

**Table de routage**
- Pour chaque destination, vérifier à quel réseau elle appartient avec le masque.
- Sinon, utiliser la `route par défaut`.

**NAT**
- Convertit `IP privée + port source` en `IP publique + nouveau port`.
- Le routeur NAT garde une `table de traduction`.
- Avantages :
- économise les IP publiques
- cache les adresses internes
- facilite le changement de FAI
- Problème classique : un hôte externe ne peut pas initier directement une connexion vers un serveur derrière le NAT sans `port forwarding` ou mécanisme équivalent.

**Routage**
- `Link State` : tous les routeurs connaissent la topologie, algorithme de `Dijkstra`.
- `Distance Vector` : échange avec les voisins, idée de `Bellman-Ford`.
- `RIP` :
- intra-AS
- distance vector
- métrique = nombre de sauts
- max `15` sauts
- annonce toutes les `30 s`
- utilise `UDP`
- `OSPF` :
- intra-AS
- link state
- utilise `Dijkstra`
- directement sur `IP`
- supporte authentification et multipath
- `BGP` :
- inter-AS
- annonce l’accessibilité aux sous-réseaux entre systèmes autonomes

**4. Couche liaison**
- Unité de données = `trame`
- Services :
- mise en trame
- accès au média
- adressage `MAC`
- détection/correction d’erreurs
- parfois transfert fiable entre nœuds adjacents
- contrôle de flux local

**Accès multiple**
- `TDMA` : chacun a son slot de temps
- `FDMA` : chacun a sa bande de fréquence
- `Polling` : un maître invite les autres
- `Passage de jeton` : droit d’émettre circule
- `Accès aléatoire` : `ALOHA`, `Slotted ALOHA`, `CSMA`, `CSMA/CD`
- Classement à retenir : `CSMA/CD` est plus performant que `Slotted ALOHA`, lui-même plus performant que `ALOHA pur`
- Les techniques qui évitent complètement les collisions : `TDMA` et `FDMA`

**Ethernet**
- Aujourd’hui surtout en `étoile` avec switch.
- Ancien bus : un seul domaine de collision.
- Ethernet est `sans connexion` et `non fiable`.
- Le switch réduit les collisions et le trafic grâce à :
- la mise en mémoire tampon
- l’apprentissage MAC
- Entête/trame :
- `préambule` : synchronisation
- `MAC destination`
- `MAC source`
- `type`
- `données`
- `CRC`
- Adresse MAC de diffusion : `FF:FF:FF:FF:FF:FF`
- Le protocole MAC Ethernet classique : `CSMA/CD`

**CSMA/CD**
- Écouter le canal
- Transmettre si libre
- Si collision : arrêter, envoyer `jam`, attendre un délai aléatoire
- `jam` = `48 temps bits`
- Backoff exponentiel binaire :
- attente = `K * 512 temps bits`
- après la `m`e collision, `K` est choisi dans `[0, 2^m - 1]`
- À `10 Mbps`, `512 temps bits = 51,2 µs`

**ARP**
- Sert à trouver l’adresse `MAC` correspondant à une `IP` sur le LAN.
- Requête ARP : `broadcast`
- Réponse ARP : `unicast`
- La table ARP est apprise automatiquement : protocole `plug-and-play`.
- Point très important :
- si la destination est dans le même sous-réseau, on cherche la MAC de la destination
- sinon, on cherche la MAC du `gateway`, pas celle de l’hôte final

**DHCP + vie d’une requête web**
Ordre logique très probable en question de compréhension :
- `DHCP` : le client obtient `IP`, `gateway`, `DNS`
- `ARP` : il découvre la MAC du routeur local
- `DNS` : il résout le nom du serveur web
- `TCP` : il fait le `3-way handshake`
- `HTTP` : il envoie la requête et reçoit la réponse

**5. Calculs et méthodes à savoir faire**
- `Transmission delay = L / R`
- `Propagation delay = d / s`
- `Queueing delay` : variable, dépend de la charge
- Le temps de transmission dépend de `la taille du paquet` et du `débit`, pas de la longueur du lien.
- Le temps de propagation dépend de `la distance` et de `la vitesse du signal`, pas de la taille du paquet.
- Avec `store-and-forward` sur 2 liens identiques : `2 * (L/C + d/s)`
- Pour les exercices TCP séq/ACK :
- `Seq` = premier octet du segment
- `Ack` = prochain octet attendu
- si un segment transporte `100 octets`, le prochain commence `+100`

**6. Pièges de quiz à mémoriser**
- `HTTP` est `sans état`.
- `HTTP persistant` permet plusieurs objets sur une même connexion TCP.
- `TCP` ne fournit pas l’encryptage.
- `TCP` ne garantit pas un délai borné.
- `UDP` ne garantit ni ordre, ni non-duplication, ni absence de pertes.
- `Checksum` ne garantit pas à 100 % l’absence d’erreur.
- `La couche réseau` est implémentée dans les hôtes et les routeurs.
- `Ethernet` n’est pas fiable.
- `Half-duplex` ne permet pas l’émission simultanée dans les deux sens.
- `PPP` est point-à-point, pas partage de canal.
- `Hub` ne règle pas les collisions.
- `Switch` travaille en couche liaison avec les `MAC`.
- `Routeur` travaille en couche réseau avec les `IP`.

**7. Priorités de révision si tu manques de temps**
- `Adressage IP / masques / VLSM / broadcast / route par défaut`
- `Fragmentation IP`
- `TCP vs UDP`, `Seq/Ack`, handshake, flow/congestion
- `Ethernet / ARP / CSMA-CD`
- `HTTP / DNS / SMTP-POP3-IMAP`
- `RIP / OSPF / BGP` au niveau conceptuel

**8. Réflexe Packet Tracer**
- Vérifie `IP`, `masque`, `gateway` sur chaque PC.
- Vérifie que chaque interface de routeur est dans le bon sous-réseau.
- Vérifie qu’une destination distante passe par le `gateway`.
- Pense `ARP pour le prochain saut`, pas nécessairement pour l’hôte final.
- Teste localement avant de tester à distance : même LAN, puis inter-réseaux.
- Si un lien routeur-routeur est présent, pense petit sous-réseau dédié, souvent `/30`.

Si tu veux, je peux faire l’une de ces 3 suites :
1. un `examen blanc ENA Quiz` de 25 questions avec corrigé
2. une `fiche ultra-condensée d’une page`
3. une `série de mises en situation Packet Tracer` typiques avec démarche de résolution