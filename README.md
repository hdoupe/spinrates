# spinrates

![image](https://user-images.githubusercontent.com/9206065/122702880-61682880-d21e-11eb-8ee7-98be24e694d9.png)

Analyze spinrate changes after foreign substance enforecments


```
conda env create -f environment.local.yml
```

```
conda activate spinrates-env
```

```
export PYBASEBALL_CACHE="`pwd`/pybaseball_cache"
python load_cache.py --storage local
```

```
streamlit run spinrates.py
```
