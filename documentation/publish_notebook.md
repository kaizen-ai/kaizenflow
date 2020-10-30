# Description

- This tool can be used for various purposes

*   Take a snapshot of a notebook as a backup or before making changes as a checkpoint
    *   This is a lightweight alternative to unit testing to capture the desired behavior of a notebook
    *   One can take a snapshot and visually compare multiple notebooks side-by-side for changes
*   Open a notebook from github, from a snapshot without using jupyter notebook (which modifies the file in your client) or github preview (which is slow or fails when the notebook is too large)
*   Use it to compare multiple notebooks against each other in different browser windows
*   Share a notebook with someone else in a simple way
*   Review someone notebook

# Options

```
```

## Publishing a snapshot of a notebook

1. Go in the same git client that your jupyter notebook is running. Note that the script assumes that you are running the jupyter server from inside a git client
2. Copy the url from the browser (e.g., `http://localhost:9186/notebooks/ravenpack/RP_data_exploration/Task217_Event_study_framework_demo.ipynb`)
3. Run

    ```
> publish_notebook.py --file http://localhost:9186/notebooks/ravenpack/RP_data_exploration/Task217_Event_study_framework_demo.ipynb --action archive
2018-08-23 01:50:09 AM <module>            : INFO  Server name=AWS_monster_server
2018-08-23 01:50:09 AM <module>            : INFO  backup path=/data/notebooks/backup
2018-08-23 01:50:09 AM <module>            : INFO  share_path=/data/notebooks/publish
2018-08-23 01:50:09 AM <module>            : INFO  Backing up ipynb
...
2018-08-23 01:50:10 AM <module>            : INFO  Html file path is: /data/notebooks/publish/Task217_Event_study_framework_demo_20180823_015009.html
```

4. The file is saved on the monster server at

```
/data/notebooks/publish/Task217_Event_study_framework_demo_20180823_015009.html
```

5. To open the snapshot locally (Mac only)
    ```
> FILE="/data/notebooks/publish/Task217_Event_study_framework_demo_20180823_015009.html/"; scp 54.172.40.4:$FILE /tmp; open /tmp/$(basename $FILE)
```

### Webserver

The webserver root is /data/notebooks/publish on the AWS monster machine

You need to open a tunnel with:


```
> ssh -N -L 8181:localhost:80 <YOUR_USER>@$AWS_MONSTER_IP
```


Then go to your local browser at 


```
http://localhost:8181
