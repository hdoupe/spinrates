# spinrates

![image](https://user-images.githubusercontent.com/9206065/122702904-6d53ea80-d21e-11eb-8de2-ab9a8e237814.png)

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
