# Outil de recherche d'expertise (_expert finder system_)

**Université Paris 1 Panthéon-Sorbonne**

L'outil de recherche d'expertise (EFS) est un POC (_proof of concept_) de moteur de recherche d'expertises en
établissement ESR assisté par l'intelligence artificielle développé par l'Université Paris 1 Panthéon-Sorbonne.
Il permet d'identifier des experts à partir d'
une requête utilisateur en langage naturel sur la base de leurs publications.
L'EFS est alimenté quotidiennement par les données de la plateforme HAL
institutionnelle. Il utilise les modèles de langage S-BERT (paraphrase-multilingual-mpnet-base-v2) et GPT-3 (ADA) de
l'API OpenAI pour calculer les similarités entre la requête utilisateur et les métadonnées des publications.
L'EFS est alimenté quotidiennement par les données de la plateforme HAL
institutionnelle. Il utilise les modèles de langage S-BERT (paraphrase-multilingual-mpnet-base-v2) et GPT-3 (ADA) de
l'API OpenAI pour calculer les similarités entre la requête utilisateur et les métadonnées des publications.

L'interface utilisateur en React est intégrée comme un widget sur le site institutionnel de l'Université Paris 1
Panthéon-Sorbonne : https://recherche.pantheonsorbonne.fr/structures-recherche/rechercher-expertise

Pour plus d'informations,
voir [cet article de l'observatoire de l'intelligence artificielle de Paris 1](https://observatoire-ia.pantheonsorbonne.fr/actualite/outil-recherche-dexpertise-base-lintelligence-artificielle-luniversite-paris-1-pantheon).

#### Avertissement

Cette application est un POC ("proof of concept"). Ce n'est pas une application pérenne et elle n'a pas vocation à être
maintenue. L'université Paris 1 panthéon Sorbonne travaille désormais sur un nouvel outil de recherche d'expertise,
baptisé Idyia, dans le cadre de son projet de système d'information recherche mutualisé.

La présente application comporte d'importantes limitations :

- limitations fonctionnelles : la recherche d'experts s'effectue exclusivement à partir de métadonnées vectorisées (
  recherche sémantique), à l'exclusion de toute recherche par mots-clés, ce qui rend difficile pour les chercheurs et
  les chercheuses le contrôle de leurs modalités d'exposition.
- limitations techniques : le code n'est pas sous _linting_ ni sous tests unitaires et la documentation est limitée
- limitations du périmètre de données : seules les données HAL sont disponibles et les affiliations ne sont connues
  qu'approximativement.

Néanmoins, cet outil de recherche d'expertise est suffisamment robuste et sécurisé pour un déploiement en production.

#### Architecture

L'EFS est une application 3 tiers :

* **efs-computing**, le backend qui assure le chargement des données Hal, les calculs sous S-BERT et les échanges avec
  l'API OpenAI
    * Technologie : Python/PyTorch/Celery
    * Repository : https://github.com/UnivParis1/efs-computing
* **efs-api**, le back office node-express
    * Technologie : Node - Express
    * Repository : https://github.com/UnivParis1/efs-api
* **efs-gui**, l'interface utilisateurs
    * Technologie : React / Mui
    * Repository : https://github.com/UnivParis1/efs-gui

#### Licence

Le code source de l'EFS est publié sous licence CECILL v2.1. Voir le fichier [LICENSE](LICENSE) pour plus de détails.

#### Déploiement du back-end efs-computing

Le présent repository efs-computing héberge le module de calcul de l'EFS. Il doit être adossé à la base de données
vectorielle Weaviate (à Paris 1
l'image docker utilisée est semitechnologies/weaviate:1.17.2).

* L'environnement est géré sous dotenv (completer tous les fichiers `.env.example` en retirant l'extension `.example`)
* Les vectorisations sémantiques des métadonnées de publications HAL sont effectuées par lot et nécessitent la
  configuration du cron.
* Les vectorisations des requêtes utilisateurs sont effectuées à la volée par des tâches celery qui doivent être lancées
  en tant que services.

##### Vectorisation des métadonnées HAL

C'est un processus en 4 tâches:

- dump_hal_csv.py : télécharge les métadonnées HAL des publications depuis le portail institutionnel et les stocke dans
  un fichier csv
- vectorize_sentences.py : vectorise les métadonnées HAL et les stocke dans des fichiers json
- weaviate_import.py : importe les données vectorisées dans la base de données Weaviate
- clean_database.py : retire de la base de données Weaviate les publications qui ne sont plus présentes dans HAL
- own_inst_patch.py : réapplique les affiliations Paris 1 sur les données déjà présentes dans Weaviate

Voici à titre indicatif la configuration du _user cron_ à Paris 1 Panthéon-Sorbonne. On note que la durée des tâches
étant significatives, elles sont lancées à des horaires décalés.

```
0 5 * * * cd /app/directory/efs/efs-computing && . venv/bin/activate && python3 dump_hal_csv.py --days 1 > /tmp/out1 2>&1
15 5 * * * cd /app/directory/efs/efs-computing && . venv/bin/activate && python3 vectorize_sentences.py --openai 1 > /tmp/out1 2>&1
00 6 * * * cd /app/directory/efs/efs-computing && . venv/bin/activate && python3 weaviate_import.py > /tmp/out1 2>&1
30 6 * * * cd /app/directory/efs/efs-computing && . venv/bin/activate && python3 clean_database.py > /tmp/out1 2>&1
45 6 * * * cd /app/directory/efs/efs-computing && . venv/bin/activate && python3 own_inst_patch.py > /tmp/out1 2>&1
```

##### Calcul des vecteurs sémantique à la volée

Efs-computing calcule à la volée les vecteurs sémantiques (embedding) via des tâches
Celery : [local_model_tasks](local_model_tasks.py) et [remote_model_tasks](remote_model_tasks.py).
Les local_model_tasks faisant appel au modèle hébergé _on premise_ (S-BERT) ont été séparées des remote_model_tasks qui
font appel à l'API GPT car les premières sont CPU-Bound et consomment une quantité significative de RAM ce qui ne permet
pas d'opter pour un niveau élevé de parallélisme alors que les secondes font surtout des entrées sorties : on gagne à
les paralléliser le plus possible.

Pour concrétiser cette approche, vous trouvez ci-dessous la configuration systemd pour le service celery-cpu qui gère
les workers celery cpu-intensive qui opèrent le modèle local et pour le service celery-io qui gère les workers qui font
appel à l'API OpenAI.

```bash
#/etc/conf.d/celery-cpu 
CELERY_APP="local_model_tasks"
CELERYD_NODES="worker"
CELERYD_OPTS="--concurrency=1 -Q qcpu"
CELERY_BIN="/app/directory/efs/efs-computing/venv/bin/celery"
CELERYD_PID_FILE="/app/directory/efs/run/celery-cpu/%n.pid"
CELERYD_LOG_FILE="/var/log/celery/celery-cpu-%n%I.log"
CELERYD_LOG_LEVEL="INFO"

#/etc/systemd/system/celery-cpu.service
[Unit]
Description=Celery Service for CPU bound service
After=network.target

[Service]
Type=forking
User=efs
Group=efs
EnvironmentFile=/etc/conf.d/celery-cpu
WorkingDirectory=/app/directory/efs/efs-computing
ExecStart=/bin/sh -c '${CELERY_BIN} -A $CELERY_APP multi start $CELERYD_NODES \
    --pidfile=${CELERYD_PID_FILE} --logfile=${CELERYD_LOG_FILE} \
    --loglevel="${CELERYD_LOG_LEVEL}" $CELERYD_OPTS'
ExecStop=/bin/sh -c '${CELERY_BIN} multi stopwait $CELERYD_NODES \
    --pidfile=${CELERYD_PID_FILE} --logfile=${CELERYD_LOG_FILE} \
    --loglevel="${CELERYD_LOG_LEVEL}"'
ExecReload=/bin/sh -c '${CELERY_BIN} -A $CELERY_APP multi restart $CELERYD_NODES \
    --pidfile=${CELERYD_PID_FILE} --logfile=${CELERYD_LOG_FILE} \
    --loglevel="${CELERYD_LOG_LEVEL}" $CELERYD_OPTS'
Restart=always

[Install]
WantedBy=multi-user.target

#/etc/conf.d/celery-io
CELERY_APP="remote_model_tasks"
CELERYD_NODES="worker"
CELERYD_OPTS="--concurrency=64 -Q qio"
CELERY_BIN="/app/directory/efs/efs-computing/venv/bin/celery"
CELERYD_PID_FILE="/app/directory/efs/run/celery-io/%n.pid"
CELERYD_LOG_FILE="/var/log/celery/celery-io-%n%I.log"
CELERYD_LOG_LEVEL="INFO"

#/etc/systemd/system/celery-io.service
[Unit]
Description=Celery Service for IO bound service
After=network.target

[Service]
Type=forking
User=efs
Group=efs
EnvironmentFile=/etc/conf.d/celery-io
WorkingDirectory=/app/directory/efs/efs-computing
ExecStart=/bin/sh -c '${CELERY_BIN} -A $CELERY_APP multi start $CELERYD_NODES \
    --pidfile=${CELERYD_PID_FILE} --logfile=${CELERYD_LOG_FILE} \
    --loglevel="${CELERYD_LOG_LEVEL}" $CELERYD_OPTS'
ExecStop=/bin/sh -c '${CELERY_BIN} multi stopwait $CELERYD_NODES \
    --pidfile=${CELERYD_PID_FILE} --logfile=${CELERYD_LOG_FILE} \
    --loglevel="${CELERYD_LOG_LEVEL}"'
ExecReload=/bin/sh -c '${CELERY_BIN} -A $CELERY_APP multi restart $CELERYD_NODES \
    --pidfile=${CELERYD_PID_FILE} --logfile=${CELERYD_LOG_FILE} \
    --loglevel="${CELERYD_LOG_LEVEL}" $CELERYD_OPTS'
Restart=always

[Install]
WantedBy=multi-user.target

```

