import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import colors

def rank_mapper(df:pd.core.dataframe.DataFrame,savepath:str,title:str="Rank of Home Cities of Hutchins Workers, 2002-2019",cityname_column:str="work_city"):
    """
    This function creates a chart of a citie's destinations formatted as a rank-change chart.
    :param DataFrame df: df formatted correctly.
    :param str title: Title you'd like to use.
    :param str cityname_column: Column with destination or origin city in it
    :param str savepath: Filename and path to output
    """
    # get top cities over time

    top_city_orig = df.reset_index().sort_values(
        by=["year", "total"], ascending=[True, False]
    )

    yr_top_city = pd.DataFrame()
    for x in top_city_orig["year"].unique():
        y = top_city_orig.query("year == @x").head(15)
        y["rank"] = range(1, 16)
        yr_top_city = pd.concat([yr_top_city, y])

    # create matrix

    yr_top_matrix = (
        yr_top_city[["year", cityname_column, "rank"]]
        .pivot(index=[cityname_column], columns=["year"])
        .fillna(0)
    )
    yr_tot_matrix = (
        yr_top_city[["year", cityname_column, "total"]]
        .pivot(index=[cityname_column], columns=["year"])
        .fillna(0)
    )

    list_of_labels_display = []
    list_of_labels_iter = []
    for y, z in zip(yr_top_matrix.columns, yr_tot_matrix.columns):
        labels = yr_top_matrix.sort_values(by=y, ascending=True)[y].drop(
            yr_top_matrix[yr_top_matrix[y] == 0].index
        )
        labels_num = yr_tot_matrix.sort_values(by=z, ascending=False)[z].drop(
            yr_tot_matrix[yr_tot_matrix[z] == 0].index
        )
        list_of_labels_iter.append(labels.index)
        list_of_labels_display.append(
            list([f"{q}\n({r:,.0f})" for q, r in zip(labels.index, labels_num)])
        )
    label_array = np.transpose(np.array(list_of_labels_iter))
    label_disp_array = np.transpose(np.array(list_of_labels_display))

    # rank change
    geoarray = label_array
    rowcount = geoarray.shape[0]
    colcount = geoarray.shape[1]

    # Create a number of blank lists
    changelist = [[] for _ in range(rowcount)]

    for i in range(colcount):
        if i == 0:
            # Rank change for 1st year is 0, as there is no previous year
            for j in range(rowcount):
                changelist[j].append(0)
        else:
            col = geoarray[:, i]  # Get all values in this col
            prevcol = geoarray[:, i - 1]  # Get all values in previous col
            for v in col:
                array_pos = np.where(col == v)  # returns array
                current_pos = int(array_pos[0])  # get first array value
                array_pos2 = np.where(prevcol == v)  # returns array
                if (
                    len(array_pos2[0]) == 0
                ):  # if array is empty, because place was not in previous year
                    previous_pos = current_pos + 1
                else:
                    previous_pos = int(array_pos2[0])  # get first array value
                if current_pos == previous_pos:
                    changelist[current_pos].append(0)
                    # No change in rank
                elif current_pos > previous_pos:  # Larger value = smaller rank
                    changelist[current_pos].append(-1)
                elif current_pos < previous_pos:  # Larger value = smaller rank
                    changelist[current_pos].append(1)
                    # Rank has decreased
                else:
                    pass

    rankchange = np.array(changelist)

    list_of_totals = []
    for y in yr_tot_matrix.columns:
        totals = yr_tot_matrix.sort_values(by=y, ascending=False)[y].drop(
            yr_tot_matrix[yr_tot_matrix[y] == 0].index
        )
        list_of_totals.append(list(totals.values))
    totals_array = np.transpose(np.array(list_of_totals))

    # make plot

    alabels = label_disp_array
    yrs = list(top_city_orig["year"].unique())
    xlabels = yrs
    ylabels = [
        "1st",
        "2nd",
        "3rd",
        "4th",
        "5th",
        "6th",
        "7th",
        "8th",
        "9th",
        "10th",
        "11th",
        "12th",
        "13th",
        "14th",
        "15th",
    ]

    mycolors = colors.ListedColormap(["#de425b", "#f7f7f7", "#67a9cf"])
    fig, ax = plt.subplots(figsize=(22, 22))
    im = ax.imshow(rankchange, cmap=mycolors)

    # Show all ticks...
    ax.set_xticks(np.arange(len(xlabels)))
    ax.set_yticks(np.arange(len(ylabels)))
    # ... and label them with the respective list entries
    ax.set_xticklabels(xlabels)
    ax.set_yticklabels(ylabels)

    # Create white grid.
    ax.set_xticks(np.arange(totals_array.shape[1] + 1) - 0.5, minor=True)
    ax.set_yticks(np.arange(totals_array.shape[0] + 1) - 0.5, minor=True)
    ax.grid(which="minor", color="gray", alpha=0.5, linestyle="-", linewidth=2)
    ax.grid(which="major", visible=False)
    ax.set_xlabel(
        "Source: Dallas College Labor Market Intelligence Center",
        loc="right",
        fontsize=13,
    )

    cbar = ax.figure.colorbar(im, ax=ax, ticks=[1, 0, -1], shrink=0.5)
    cbar.ax.set_yticklabels(
        ["Higher Rank YOY", "No Change", "Lower Rank YOY"], fontsize=18
    )

    # Loop over data dimensions and create text annotations.
    for i in range(len(ylabels)):
        for j in range(len(xlabels)):
            lab = alabels[i, j].split(" ")
            if len(lab) > 1:
                label = lab[0] + "\n " + " ".join(lab[1:])
            else:
                label = lab[0]
            text = ax.text(
                j, i, label, ha="center", va="center", color="black", fontsize=10.25
            )
    ax.set_title(title, fontsize=20)
    plt.xticks(fontsize=18)
    plt.yticks(fontsize=18)
    fig.tight_layout()
    plt.savefig(savepath, dpi=600, facecolor="None")
    plt.show()
