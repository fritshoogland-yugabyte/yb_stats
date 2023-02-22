In order to use the fritshoogland/yb_stats:latest container, run:

```shell
docker pull fritshoogland/yb_stats:latest
```
To pull the yb_stats container, and then:
```shell
docker run --rm -it -v $(pwd):/app yb_stats
```
Add arguments at the end of the command.
The volume mount (`-v`) mounts the local current working directory, 
so the container read and write the `.env` file, and read and write snapshots.

For windows this is almost the same, only the volume mount must be changed.
If anyone finds a way to use its current working directory 
