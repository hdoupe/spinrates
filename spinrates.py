from datetime import datetime
from pathlib import Path

import streamlit as st
from s3fs import S3FileSystem
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


st.title("Spin Rates")

st.write("Gathering data...")


@st.cache
def load_cache():
    local_cache_path = Path("statcast_data.parquet")
    if local_cache_path.exists():
        cache_path = local_cache_path
        print(f"Reading data from cache: {cache_path}")
        df = pd.read_parquet(cache_path, engine="fastparquet")
    else:
        cache_path = "s3://pybaseball-spinrates-cache/statcast_data.parquet"
        print(f"Reading data from cache: {cache_path}")
        fs = S3FileSystem(anon=True)
        with fs.open(cache_path, "rb") as f:
            df = pd.read_parquet(f, engine="fastparquet")
        
    return df


START_DATE = "2021-04-01"
TODAY = str(datetime.now().date())
ENFORCEMENT_DATE = "2021-06-15"

print("Loading data from cache now.")
data = load_cache()
print(data.tail())

st.write(data.head())


ids = data[["pitcher", "player_name"]].copy()
ids.drop_duplicates(inplace=True)
ids_dict = ids.set_index("pitcher").to_dict("dict")["player_name"]

st.write("Classifying pitches...")


pitch_types = ", ".join(
    val for val in data["pitch_type"].unique() if isinstance(val, str)
)

st.write(f"Found pitch types: {pitch_types}")


st.write("Splitting data into pre and post enforcement groups...")
pre_data = data[data["game_date"] < ENFORCEMENT_DATE].copy()
post_data = data[data["game_date"] >= ENFORCEMENT_DATE].copy()

pre_gdf = pre_data.groupby(["pitch_class", "pitcher"])["release_spin_rate"].mean()
post_gdf = post_data.groupby(["pitch_class", "pitcher"])["release_spin_rate"].mean()

# find number of pitches thrown. used to set minimum threshold
npitches_pre = pre_data.groupby("pitcher")["pitcher"].count()
npitches_pre = pd.DataFrame(npitches_pre)
npitches_pre.columns = ["npitches"]
npitches_pre.reset_index(inplace=True)
npitches_pre

ax = npitches_pre["npitches"].hist(bins=50)
fig = ax.get_figure()


st.write("## Pitcher level")
st.write("### Breakdown of number of pitches for each pitcher")
st.pyplot(fig)


npitches_post = post_data.groupby("pitcher")["pitcher"].count()
npitches_post = pd.DataFrame(npitches_post)
npitches_post.columns = ["npitches"]
npitches_post.reset_index(inplace=True)
# npitches_post

st.write(
    "Summary stats on number of pitches for each pitcher in post enforcement data:"
)
st.write(npitches_post["npitches"].describe())

pd.DataFrame(pre_gdf).reset_index()


st.write("Dropping pitchers in lowest 25% of number pitches thrown")
pre_gdf = pd.DataFrame(pre_gdf).reset_index()
post_gdf = pd.DataFrame(post_gdf).reset_index()
# merge on pitch counts
pre_gdf = pre_gdf.merge(npitches_pre, on=["pitcher"])
post_gdf = post_gdf.merge(npitches_post, on="pitcher")
# drop the bottom 25% of pitchers
pre_gdf = pre_gdf[pre_gdf["npitches"] >= pre_gdf["npitches"].describe().loc["25%"]]
post_gdf = post_gdf[post_gdf["npitches"] >= post_gdf["npitches"].describe().loc["25%"]]


st.write("Merging pre and post data...")
full_data = pd.merge(
    pre_gdf, post_gdf, on=["pitcher", "pitch_class"], suffixes=["_pre", "_post"]
)
full_data.rename(
    columns={
        "release_spin_rate_pre": "pre_spin",
        "release_spin_rate_post": "post_spin",
    },
    inplace=True,
)
full_data.head()


fig, ax = plt.subplots()
ax.scatter(full_data["pre_spin"], full_data["post_spin"])
ax.set_xlabel("Pre-Enforcement Spin Rate")
ax.set_ylabel("Post-Enforcement Spin Rate")
min_val = full_data[["pre_spin", "post_spin"]].min()
max_val = full_data[["pre_spin", "post_spin"]].max()
ax.plot([min_val, max_val], [min_val, max_val])

st.write("### Pitch spin-rates pre vs. post enforcement")
st.pyplot(fig)


def scatter(data, pitch_class, title):
    data = data[data["pitch_class"] == pitch_class]
    fig, ax = plt.subplots()
    ax.scatter(data["pre_spin"], data["post_spin"])
    ax.set_xlabel("Pre-Enforcement")
    ax.set_ylabel("Post-Enforcement")
    ax.set_title(title)
    min_val = data[["pre_spin", "post_spin"]].min()
    max_val = data[["pre_spin", "post_spin"]].max()
    ax.plot([min_val, max_val], [min_val, max_val])
    return fig


st.pyplot(scatter(full_data, "fastball", "Average Fastball Spin rate"))
st.pyplot(scatter(full_data, "offspeed", "Average Off-Speed Spin Rate"))

full_data["diff"] = full_data["post_spin"] - full_data["pre_spin"]
full_data["pct_change"] = full_data["diff"] / full_data["pre_spin"] * 100
full_data["player"] = full_data["pitcher"].map(ids_dict)

st.write("### Pitchers with greatest pct. change in fastball spin rate")
st.write(
    full_data[full_data["pitch_class"] == "fastball"].sort_values("pct_change").head(10)
)

st.write("### Pitchers with greatest pct. change in offspeed spin rate")
st.write(
    full_data[full_data["pitch_class"] == "offspeed"].sort_values("pct_change").head(10)
)

st.write("## Team level")
st.write("Creating team level data...")
pre_data["team"] = np.where(
    pre_data["inning_topbot"] == "Top", pre_data["home_team"], pre_data["away_team"]
)
post_data["team"] = np.where(
    post_data["inning_topbot"] == "Top", post_data["home_team"], post_data["away_team"]
)

team_pre = pre_data.groupby(["pitch_class", "team"])["release_spin_rate"].mean()
team_post = post_data.groupby(["pitch_class", "team"])["release_spin_rate"].mean()
team_data = pd.merge(team_pre, team_post, left_index=True, right_index=True)
team_data.columns = ["pre_spin", "post_spin"]
team_data.reset_index(inplace=True)
team_data.head()

team_fig, team_ax = plt.subplots()
colors = {"fastball": "blue", "offspeed": "orange", "other": "green"}
team_ax.scatter(
    x=team_data["pre_spin"][team_data["pitch_class"] == "fastball"],
    y=team_data["post_spin"][team_data["pitch_class"] == "fastball"],
    label="Fastballs",
)
team_ax.scatter(
    x=team_data["pre_spin"][team_data["pitch_class"] == "offspeed"],
    y=team_data["post_spin"][team_data["pitch_class"] == "offspeed"],
    label="Off-Speed",
)
team_ax.set_xlabel("Pre-Enforecement Spin Rate")
team_ax.set_ylabel("Post-Enforecement Spin Rate")
team_ax.legend(loc="lower right")
min_val = team_data[["pre_spin", "post_spin"]].min()
max_val = team_data[["pre_spin", "post_spin"]].max()
team_ax.plot([min_val, max_val], [min_val, max_val], c="black")
team_ax.set_title("Team Spin Rates")
st.pyplot(team_fig)


team_data["diff"] = team_data["post_spin"] - team_data["pre_spin"]
team_data["pct_change"] = team_data["diff"] / team_data["pre_spin"] * 100
st.write("### Fastball spin rate changes")
st.write(team_data[team_data["pitch_class"] == "fastball"].sort_values("pct_change"))

st.write("### Offspeed spin rate changes")
team_data[team_data["pitch_class"] == "offspeed"].sort_values("pct_change")

fb_data = team_data[team_data["pitch_class"] == "fastball"]
fb_fig, fb_ax = plt.subplots()
fb_ax.set_xlim((fb_data["pre_spin"].min() * 0.98), (fb_data["pre_spin"].max()) * 1.02)
fb_ax.set_ylim((fb_data["post_spin"].min() * 0.98), (fb_data["post_spin"].max()) * 1.02)
# for x, y, s in zip(fb_data['pre_spin'], fb_data['post_spin'], fb_data['team']):
#     text_ax.text(x, y, s)
for x, y, s in zip(fb_data["pre_spin"], fb_data["post_spin"], fb_data["team"]):
    fb_ax.text(x, y, s, c="blue")
# 45 degree line
min_val = fb_data[["pre_spin", "post_spin"]].min()
max_val = fb_data[["pre_spin", "post_spin"]].max()
fb_ax.plot([min_val, max_val], [min_val, max_val], c="black", linewidth=0.5)
fb_ax.set_xlabel("Pre-Enforcement")
fb_ax.set_ylabel("Post-Enforcement")
fb_ax.set_title("Average Fastball Spin Rate")

st.pyplot(fb_fig)

os_data = team_data[team_data["pitch_class"] == "offspeed"]
os_fig_, os_ax = plt.subplots()
os_ax.set_xlim((os_data["pre_spin"].min() * 0.98), (os_data["pre_spin"].max()) * 1.02)
os_ax.set_ylim((os_data["post_spin"].min() * 0.98), (os_data["post_spin"].max()) * 1.02)

for x, y, s in zip(os_data["pre_spin"], os_data["post_spin"], os_data["team"]):
    os_ax.text(x, y, s, c="blue")
# 45 degree line
min_val = os_data[["pre_spin", "post_spin"]].min()
max_val = os_data[["pre_spin", "post_spin"]].max()
os_ax.plot([min_val, max_val], [min_val, max_val], c="black", linewidth=0.5)
os_ax.set_xlabel("Pre-Enforcement")
os_ax.set_ylabel("Post-Enforcement")
os_ax.set_title("Average Off-Speed Spin Rate")
st.pyplot(os_fig_)
