import requests
import json
import pandas as pd
import geopandas as gpd
from urllib.parse import urljoin, urlencode
from datetime import datetime
from time import sleep
import numpy as np

from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.lines import Line2D

import modules.cdms_reader as cdms_reader


def execute_matchup_request(matchup_url, query_params, in_situ_variable_name=None):
    TIMEOUT = 15 * 60
    full_matchup_url = f'{matchup_url}?{urlencode(query_params)}'
    print(full_matchup_url)

    response = requests.get(full_matchup_url)
    response_json = response.json()

    start = datetime.utcnow()
    job_url = [link for link in response_json['links'] if link['rel'] == 'self'][0]['href']
    job_id = response_json['executionID']
    print(f'Execution ID: {job_id}')

    while response_json['status'] == 'running' and (datetime.utcnow() - start).total_seconds() <= TIMEOUT:
        status_response = requests.get(job_url)
        response_json = status_response.json()

        if response_json['status'] == 'running':
            sleep(10)

    job_status = response_json['status']

    if job_status == 'running':
        print(f'Job {job_id} has been running too long, will not wait anymore')
    elif job_status in ['cancelled', 'failed']:
        raise ValueError(f'Async matchup job finished with incomplete status ({job_status})')
    else:
        results_url = [
            link for link in response_json['links'] if 'output=JSON' in link['href']
            # link for link in response_json['links'] if link['type'] == 'application/json'
        ][0]['href']

        results_response = requests.get(results_url).json()

    primary_points = []
    secondary_points = []
    for primary_point in results_response['data']:
        for variable in primary_point['matches'][0]['secondary']:
            if in_situ_variable_name:
                if variable['variable_name'] == in_situ_variable_name:
                    primary_points.append((float(primary_point['lon']), float(primary_point['lat']),
                                           float(primary_point['primary'][0]['variable_value'])))
                    secondary_points.append((float(primary_point['matches'][0]['lon']),
                                             float(primary_point['matches'][0]['lat']),
                                             float(variable['variable_value'])))
            else:
                # pick the first variable
                primary_points.append((float(primary_point['lon']), float(primary_point['lat']),
                                       float(primary_point['primary'][0]['variable_value'])))
                secondary_points.append((float(primary_point['matches'][0]['lon']),
                                         float(primary_point['matches'][0]['lat']), float(variable['variable_value'])))
                break
    print(f'Total number of primary matched points {len(primary_points)}')
    print(f'Total number of secondary matched points {len(secondary_points)}')
    return primary_points, secondary_points, job_id


def plot_points(primary_points, secondary_points):
    plt.figure(figsize=(20, 5), dpi=500)
    min_lon = min([point[0] for point in primary_points])
    max_lon = max([point[0] for point in primary_points])
    min_lat = min([point[1] for point in primary_points])
    max_lat = min([point[1] for point in primary_points])
    basemap = Basemap(
        projection='mill',
        lon_0=180,
        llcrnrlat=min_lat - 50,
        urcrnrlat=max_lat + 50,
        llcrnrlon=min_lon - 50,
        urcrnrlon=max_lon + 50
    )
    basemap.drawlsmask(
        land_color='lightgrey',
        ocean_color='white',
        lakes=True
    )

    # transform coordinates
    x1, y1 = basemap([point[0] for point in primary_points], [point[1] for point in primary_points])
    x2, y2 = basemap([point[0] for point in secondary_points], [point[1] for point in secondary_points])

    # Draw scatter points
    plt.scatter(x2, y2, 10, marker='o', color='Blue', label='Primary point')
    plt.scatter(x1, y1, 10, marker='*', color='Green', label='Secondary point')

    # transform input bbox
    bx, by = basemap([-140, -110], [40, 10])

    # Draw user provided bounds
    plt.gca().add_patch(patches.Rectangle(
        (bx[0], by[1]), abs(bx[0] - bx[1]), abs(by[0] - by[1]),
        linewidth=1,
        edgecolor='r',
        facecolor='none'
    ))

    # Show legend
    handles, labels = plt.gca().get_legend_handles_labels()
    bbox_legend = Line2D(
        [0], [0],
        color='red',
        linewidth=1,
        linestyle='-',
        label='User search domain'
    )
    handles.append(bbox_legend)
    plt.legend(loc='upper left', handles=handles)

    plt.show()


def generate_diff_plot(primary_points, secondary_points, primary_name, secondary_name, units, title):
    diffs = [primary_point[2] - secondary_point[2] for primary_point, secondary_point in
             zip(primary_points, secondary_points)]

    plt.figure(figsize=(20, 5), dpi=500)
    min_lon = min([point[0] for point in primary_points])
    max_lon = max([point[0] for point in primary_points])
    min_lat = min([point[1] for point in primary_points])
    max_lat = min([point[1] for point in primary_points])
    basemap = Basemap(
        projection='mill',
        lon_0=180,
        llcrnrlat=min_lat - 5,
        urcrnrlat=max_lat + 30,
        llcrnrlon=min_lon - 10,
        urcrnrlon=max_lon + 10
    )
    basemap.drawlsmask(
        land_color='lightgrey',
        ocean_color='white',
        lakes=True
    )

    # transform coordinates
    x1, y1 = basemap([point[0] for point in primary_points], [point[1] for point in primary_points])
    x2, y2 = basemap([point[0] for point in secondary_points], [point[1] for point in secondary_points])

    # Customize colormap/colorbar
    cmap = plt.cm.coolwarm

    # Draw scatter points
    sc = plt.scatter(x2, y2, 30, marker='o', c=diffs, alpha=0.7, cmap=cmap)

    cb = plt.colorbar(sc)
    cb.ax.set_title(units, fontsize=8)
    plt.title(title, fontsize=8)


def fetch_result(execution_id, output_format, output_file, primary_variable, secondary_variable):
    match_up_results_csv = f'{output_file}.csv'
    response = requests.get("https://doms.jpl.nasa.gov/cdmsresults",
                            params={"id": execution_id, "output": output_format, "pageSize": 5000})
    with open(output_file, mode='wb') as f:
        f.write(response.content)

    matches = cdms_reader.assemble_matches(output_file)

    cdms_reader.matches_to_csv(matches, match_up_results_csv)

    columns_to_include = ["PrimaryData_lon", "PrimaryData_lat", "PrimaryData_datetime", "SecondaryData_lon",
                          "SecondaryData_lat", "SecondaryData_datetime"]
    for variable in primary_variable:
        columns_to_include.append(f'PrimaryData_{variable}')
    for variable in secondary_variable:
        columns_to_include.append(f'SecondaryData_{variable}')
    return pd.read_csv(match_up_results_csv, usecols=columns_to_include)


def generate_scatter_plot(primary_points, secondary_points, primary_name, secondary_name, variable_name, units):
    x = np.array([point[2] for point in secondary_points])
    y = np.array([point[2] for point in primary_points])
    m, b = np.polyfit(x, y, 1)
    fig, ax = plt.subplots()
    ax.set_title(f'{variable_name} scatter\n{primary_name} vs. {secondary_name}')
    ax.set_xlabel("%s %s" % (secondary_name, units))
    ax.set_ylabel("%s %s" % (primary_name, units))
    ax.scatter(x, y)
    ax.plot(x, m*x+b);
    ax.plot([0,1],[0,1], transform=ax.transAxes)
