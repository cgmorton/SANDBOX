import os
import datetime as dt
import logging
import json
import urllib2
import copy
import subprocess
import csv
import random

import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import session as session_module
from sqlalchemy import inspect
from shapely.geometry import asShape
from shapely.geometry import mapping
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2.shape import from_shape, to_shape
from geoalchemy2.types import Geometry
import geojson

import config

Base = declarative_base()
#######################################
# OpenET database tables
#######################################
class User(Base):
    __tablename__ = 'user'
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String())
    email = db.Column(db.String())
    last_login = db.Column(db.DateTime())
    joined =  db.Column(db.DateTime())
    ip = db.Column(db.String())
    password = db.Column(db.String())
    notes = db.Column(db.String())
    active = db.Column(db.String())
    role = db.Column(db.String())

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Region(Base):
    # States, Counties, HUCs or fields or custom
    __tablename__ = 'region'
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String())

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Dataset(Base):
    __tablename__ = 'dataset'
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String())
    ee_collection = db.Column(db.String())

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Variable(Base):
    __tablename__ = 'variable'
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String())
    units = db.Column(db.String())

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Geom(Base):
    __tablename__ = 'geom'
    id = db.Column(db.Integer(), primary_key=True)
    user_id = db.Column(db.Integer())
    year = db.Column(db.Integer())
    region_id = db.Column(db.Integer())
    feature_index =  db.Column(db.Integer())
    name = db.Column(db.String())
    type = db.Column(db.String())
    area = db.Column(db.Float(precision=4))
    coords = db.Column(Geometry(geometry_type='MULTIPOLYGON'))
    '''
    FIX ME: I don't know how to implement that in dbSCHEMA or pgADMIN
    meta = relationship('GeomMetadata', backref='geom', lazy=True)
    data = relationship('Data', backref='data', lazy=True)
    '''
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class GeomMetadata(Base):
    __tablename__ = 'geom_metadata'
    id = db.Column(db.Integer(), primary_key=True)
    geom_id = db.Column(db.Integer(), db.ForeignKey('geom.id'), nullable=False)
    name = db.Column(db.String())
    properties = db.Column(db.String())

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Parameter(Base):
    __tablename__ = 'parameter'
    id = db.Column(db.Integer(), primary_key=True)
    dataset_id = db.Column(db.Integer(), db.ForeignKey('dataset.id'), nullable=False)
    name = db.Column(db.String())
    properties =  db.Column(db.String())

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Data(Base):
    __tablename__ = 'data'
    id = db.Column(db.Integer(), primary_key=True)
    geom_id = db.Column(db.Integer(), db.ForeignKey('geom.id'), nullable=False)
    geom_name = db.Column(db.String()) #should be foreign key tp geom.name but gives error when creating
    geom_area = db.Column(db.Float(precision=4))
    year = db.Column(db.Integer())
    dataset_id =  db.Column(db.Integer(), db.ForeignKey('dataset.id'), nullable=False)
    variable_id =  db.Column(db.Integer(), db.ForeignKey('variable.id'), nullable=False)
    temporal_resolution = db.Column(db.String())
    data_date = db.Column(db.DateTime())
    data_value = db.Column(db.Float(precision=4))

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
#######################################
# END OpenET database tables
#######################################
class database_Util(object):
    '''
    Class to support database (cloudSQL) population
    Method:
        - The base query is defined from relevant template values
    Args:
        :region Unique ID of geojson obbject, e.g. USFields
        :dataset MODSI, Landsat or gridMET
        :year year of geojson dataset, might be ALL if not USFields
            USField geojsons change every year
    '''
    def __init__(self, region, dataset, year, user_id, db_engine, region_changing_by_year):
        self.region = region
        self.year = int(year)
        self.dataset = dataset
        self.user_id = user_id
        self.geo_bucket_url = config.GEO_BUCKET_URL
        self.data_bucket_url = config.DATA_BUCKET_URL
        self.db_engine = db_engine
        self.region_changing_by_year = region_changing_by_year
        # Used to read geometry data from buckets
        if self.region in ['Mason', 'US_fields']:
            # Field boundaries depend on years
            self.geoFName = region + '_' + year + '_GEOM.geojson'
        else:
            self.geoFName = region + '_GEOM.geojson'
        self.dataFName = region + '_' + year + '_DATA'  '.json'



    def start_session(self):
        # Set up the db session
        Session = session_module.sessionmaker()
        Session.configure(bind=self.db_engine)
        self.session = Session()

    def end_session(self):
        self.session.close()

    def object_as_dict(self, obj):
        '''
        Converts single db query object to dict
        :param obj:
        :return: query dict
        '''
        return {c.key: getattr(obj, c.key)
                for c in inspect(obj).mapper.column_attrs}

    def set_shell_flag(self):
        if os.name == 'posix':
            shell_flag = False
        else:
            shell_flag = True
        return shell_flag

    def upload_file_to_bucket(self, upload_path, bucket_path):
        '''
        :param upload_path: source file path on local host
        :param bucket_path: destination file path
        :return:
        '''
        logging.info('Uploading to bucket')
        shell_flag = self.set_shell_flag()
        args = ['gsutil', 'cp', upload_path, bucket_path]
        if not logging.getLogger().isEnabledFor(logging.DEBUG):
            args.insert(1, '-q')

        # Check that the file is not already in bucket
        args = ['gsutil', 'ls', bucket_path]
        try:
            out = subprocess.check_output(args, shell=shell_flag)
        except Exception as e:
            out = ''
            logging.exception('Error checking file in bucket: ' + str(e))

        if not out:
            # Add file to bucket
            try:
                subprocess.check_output(args, shell=shell_flag)
            except Exception as e:
                logging.exception('Error uploading to bucket: ' + str(e))

    def delete_file_from_local(self, upload_path):
        try:
            os.remove(upload_path)
            logging.info('Deleted local file ' + upload_path)
        except:
            pass

    def delete_file_from_bucket(self, bucket_path):
        logging.info('Deleting bucket')
        shell_flag = self.set_shell_flag()
        args = ['gsutil', 'rm', bucket_path]
        if not logging.getLogger().isEnabledFor(logging.DEBUG):
            args.insert(1, '-q')
        try:
            subprocess.check_output(args, shell=shell_flag)
        except Exception as e:
            logging.exception('Error uploading to bucket: ' + str(e))


    def read_geodata_from_bucket(self):
        '''
        All geometry data are stored in cloud buckets
        :return:
        '''
        url = self.geo_bucket_url + self.geoFName
        try:
            d = geojson.load(urllib2.urlopen(url))
        except Exception as e:
            logging.error(e)
            raise Exception(e)
        return d

    def read_etdata_from_bucket(self):
        '''
        All et data are stored in cloud buckets
        :return:
        '''
        url = self.data_bucket_url + self.dataset + '/' + self.dataFName
        print('Reading data from bucket file ' + url)
        d = json.load(urllib2.urlopen(url))
        '''
        try:
            d = json.load(urllib2.urlopen(url))
        except Exception as e:
            logging.error(e)
            raise Exception(e)
        '''
        return d

    def add_in_chunks(self, entity_list):
        ent_len = len(entity_list)
        num_chunks = ent_len / 500
        if ent_len % 500 != 0:
            end_chunk_len = ent_len % 500
            num_chunks += 1
        num_added = 0
        count = 0
        while num_added < ent_len:
            count += 1
            logging.info('ADDING CHUNK {0} of {1}'.format(str(count), str(num_chunks)))
            start = num_added
            end = start + 500
            if end > ent_len:
                end = start + end_chunk_len
            entities = entity_list[start:end]

            db.session.add_all(entities)
            try:
                db.session.commit()
            except:
                db.session.rollback()
                raise
            num_added = end

    def check_if_data_in_db(self, f_idx):
        # Check if this entry is already in db
        in_db =  False
        geom_name = self.region + '_' + str(f_idx)
        QU = query_Util({
            'region': self.region,
            'dataset': self.dataset,
            'year': self.year,
            'temporal_resolution': 'monthly',
            'variable': 'et',
            'feature_index_list': [f_idx]
        }, self.db_engine, self.session)
        '''
        NOTE: if we don't json.loads here, 
        len(json_data) is always > 0, hence in_db will always be true
        '''
        in_db = QU.check_if_data_in_db(geom_name)
        QU.end_session()
        return in_db

    def check_if_geom_in_db(self, region, f_idx, year):
        geom_query = self.session.query(Geom).filter(
            Geom.region_id == config.statics['db_id_region'][region],
            Geom.feature_index == int(f_idx),
            Geom.year == year
        )
        if len(geom_query.all()) == 0:
            return None, None
        if len(geom_query.all()) > 1:
            logging.error('Multiple geometries for ' + region + '/' + str(f_idx) + '/' + str(year))
            return -9999, None
        geom = geom_query.first()
        geom_id = geom.id
        geom_area = geom.area
        return geom_id, geom_area

    def set_postgis_geometry(self, shapely_geom):
        postgis_geom = None
        if shapely_geom.geom_type == 'Polygon':
            # convert to multi polygon
            postgis_geom = from_shape(MultiPolygon([shapely_geom]))
        elif shapely_geom.geom_type == 'MultiPolygon':
            # Save as is
            postgis_geom = from_shape(shapely_geom)
        return postgis_geom

    def set_user_entity(self, user_dict):
        return User(**user_dict)

    def set_region_entity(self, region_dict):
        return Region(**region_dict)

    def set_dataset_entity(self, dataset_dict):
        return Dataset(**dataset_dict)

    def set_variable_entity(self, variable_dict):
        return Variable(**variable_dict)

    def set_geom_metadata_entity(self, metadata_dict):
        return GeomMetadata(**metadata_dict)

    def set_and_add_geom_entity(self, feat_idx, geom_name, geom_type, postgis_geom, year, area):
        '''
        Adds the geometry row to database and retrieves the automatically
        assigned primary key geom_id
        # Note: primary key is AUTOSET in db
        '''
        geometry = Geom(
            user_id=self.user_id,
            year=int(year),
            region_id=config.statics['db_id_region'][self.region],
            feature_index=feat_idx,
            name=geom_name,
            type=geom_type,
            area=area,
            coords=postgis_geom
        )
        # Submit the geom table to obtain the primary key geom_id
        # geometry = Geom(**geom_init)
        self.session.add(geometry)
        try:
            self.session.commit()
        except:
            self.session.rollback()
            raise
        geom_id = geometry.id
        return geom_id


    def set_parameter_entity(self, parameter_dict):
        return Parameter(**parameter_dict)

    def set_data_entity(selfself, data_dict):
        return Data(**data_dict)



    def add_data_to_db(self):
        '''
        Add data to database
        :return:
        '''
        # Read etdata from bucket
        etdata = self.read_etdata_from_bucket()
        geojson_data = self.read_geodata_from_bucket()

        # Check if database is empty
        # If not empty, we need to check if entries are already in db
        db_empty = False
        self.start_session()

        q = self.session.query(Data).first()
        if q is None:
            db_empty = True

        if db_empty:
            # Set up region, dataset, parameter and variable tables
            print('Database empty, setting up basic data tables')

            # Regions
            entities = []
            for key in config.statics['db_id_region'].keys():
                init_dict = {
                    'id': config.statics['db_id_region'][key],
                    'name': key
                }
                entities.append(self.set_region_entity(init_dict))

            self.session.add_all(entities)
            try:
                self.session.commit()
            except:
                self.session.rollback()
                raise
            print('Added Region rows')

            # Datasets
            entities = []
            for key in config.statics['db_id_dataset'].keys():
                init_dict = {
                    'id': config.statics['db_id_dataset'][key],
                    'name': key,
                    'ee_collection': config.statics['ee_collection'][key],
                }
                entities.append(self.set_dataset_entity(init_dict))

            self.session.add_all(entities)
            try:
                self.session.commit()
            except:
                self.session.rollback()
                raise
            print('Added Dataset rows')

            # Parameters
            entities = []
            for key in config.statics['db_id_parameters_by_dataset'].keys():
                params = config.statics['db_id_parameters_by_dataset'][key]
                for param in params:
                    init_dict = {
                        'id': config.statics['db_id_parameter'][param],
                        'name': param,
                        'properties': '',
                    }
                    entities.append(self.set_parameter_entity(init_dict))

            self.session.add_all(entities)
            try:
                self.session.commit()
            except:
                self.session.rollback()
                raise
            print('Added Parameter rows')

            # Variables
            entities = []
            for key in config.statics['db_id_variable'].keys():
                init_dict = {
                    'id': config.statics['db_id_variable'][key],
                    'name': key,
                    'units': config.statics['units'][key],
                }
                entities.append(self.set_variable_entity(init_dict))

            self.session.add_all(entities)
            try:
                self.session.commit()
            except:
                self.session.rollback()
                raise
            print('Added Variable rows')

        # Loop over features in bucket file, do in chunks
        # Oherwise we get a kill9 error
        chunk_size = config.statics['ingest_chunk_size']
        if chunk_size <= len(etdata['features']):
            num_chunks = len(etdata['features']) / chunk_size
        else:
            num_chunks = 1

        if len(etdata['features']) / chunk_size:
            num_chunks += 1
        chunk = 1
        print('Adding data in ' + str(num_chunks) + ' chunk(s) to database.')
        # Open db connection
        # Needed to bulk copy from csv
        conn = self.session.connection()  # SQLAlchemy Connection
        dbapi_conn = conn.connection  # DBAPI connection (technically a connection pool wrapper called ConnectionFairy, but everything is there)
        cursor = dbapi_conn.cursor()  # actual DBAPI cursor

        while chunk <= num_chunks:
            csv_metadata = open('metadata.csv', 'wb+')
            csv_data = open('data.csv', 'wb+')
            csv_mwriter = csv.writer(csv_metadata, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            csv_dwriter = csv.writer(csv_data, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            idx_start = (chunk - 1) * chunk_size
            idx_end = chunk * chunk_size
            if idx_end > len(etdata['features']):
                idx_end = len(etdata['features'])
            for f_idx in range(idx_start, idx_end):
                feat_idx = f_idx +1
                # Find the geometry name
                geom_name = 'Not found'
                for name_prop in config.statics['geom_name_keys']:
                    try:
                        geom_name = geojson_data['features'][f_idx]['properties'][name_prop]
                        geom_name = geom_name.encode('utf-8').strip()
                    except:
                        continue
                in_db = False
                if not db_empty:
                    in_db = self.check_if_data_in_db(f_idx)
                if in_db:
                    print(geom_name + '/' + str(self.year)  + ' data found in db. Skipping...')
                    continue

                f_data = etdata['features'][f_idx]
                g_data = geojson_data['features'][f_idx]

                print('Adding Feature '  + str(f_idx + 1))

                # Geometry data table
                # check if the geometry is already in the database
                if self.region_changing_by_year:
                    year = self.year
                else:
                    year = 9999
                geom_id, geom_area = self.check_if_geom_in_db(self.region, feat_idx, year)
                if not geom_id:
                    # Convert the geojson geometry to postgis geometry using shapely
                    # Note: we convert polygons to multi polygon
                    # Convert to shapely shape
                    shapely_geom = asShape(g_data['geometry'])
                    geom_area = round(shapely_geom.area, 4)
                    postgis_geom = self.set_postgis_geometry(shapely_geom)
                    if postgis_geom is None:
                        raise Exception('Not a valid geometry, must be polygon or multi polygon!')
                    # Add the geometry table entry for this feature and obtain the geometry id
                    geom_id = self.set_and_add_geom_entity(feat_idx, geom_name, shapely_geom.geom_type, postgis_geom, year, geom_area)

                logging.info('Added Geometry table')
                print('Added Geometry row')
                # Set the geometry metadata and data tables for bulk ingest
                for key in config.statics['geom_meta_cols'][self.region]:
                    try:
                        value = str(g_data['properties'][key])
                    except:
                        try:
                           value = str(f_data['properties'][key])
                        except:
                            value = 'Not Found'
                    # Remove commas, causes issues when copy_from
                    value = ' '.join(value.replace(', ', ',').split(','))
                    csv_mwriter.writerow([geom_id, key, value])

                dataset_id = config.statics['db_id_dataset'][self.dataset]
                # Variable loop
                for var in config.statics['all_variable'][self.dataset].keys():
                    variable_id =  config.statics['db_id_variable'][var]
                    # Populate the data columns
                    for t_res in config.statics['all_temporal_resolution'].keys():
                        temporal_resolution = t_res
                        for data_var in config.statics['var_data_by_tres'][t_res]:
                            # Set date
                            DU = date_Util()
                            data_date = DU.get_dbtable_datetime(self.year, t_res, data_var)
                            # Set data value
                            try:
                                data_value = float(f_data['properties'][var + '_' + data_var])
                            except:
                                data_value = -9999
                            row = [geom_id, geom_name, geom_area, self.year, dataset_id, variable_id, temporal_resolution, data_date, data_value]
                            csv_dwriter.writerow(row)
            csv_metadata.close()
            csv_data.close()

            # Commit the geom metadata and data for all features
            # NOTE: committing all kills 9, try chunking
            with open('data.csv', 'r') as f:
                if os.stat("data.csv").st_size != 0:
                    cols = ('geom_id', 'geom_name', 'geom_area', 'year', 'dataset_id', 'variable_id', 'temporal_resolution', 'data_date', 'data_value')
                    cursor.copy_from(f, 'data', sep=',', columns=cols)
                    print('Added Data tables for features')

            with open('metadata.csv', 'r') as f:
                if os.stat("metadata.csv").st_size != 0:
                    cols = ('geom_id', 'name', 'properties')
                    cursor.copy_from(f, 'geom_metadata', sep=',', columns=cols)
                    print('Added GeomMetadata table rows for features')
            os.remove('metadata.csv')
            os.remove('data.csv')
            chunk += 1

        # Commit all additions to database
        self.session.commit()
        # Close the connection and session
        self.end_session()
        conn.close()

class date_Util(object):

    def get_month(self, t_res, data_var):
        '''
        :param t_res: temporal resolution
        :param data_var:  data variable found in data files: for monthly m01, m02, ect.
        :return:
        '''
        if t_res == 'annual':
            m = 12
        elif t_res == 'seasonal':
            m = 10
        elif t_res == 'monthly':
            try:
                m = int(data_var.split('m')[1])
            except:
                m = int(data_var)
        else:
            m = 12
        return m

    def set_datetime_dates_list(self, tv_vars):
        dates_list = []
        data_vars = []
        t_res = tv_vars['temporal_resolution']
        yr = int(tv_vars['year'])
        if t_res == 'annual':
            data_vars = ['annual']
        if t_res == 'seasonal':
            data_vars = ['seasonal']
        if t_res == 'monthly':
            months = tv_vars['months']
            if len(months) == 1 and months[0] == 'all':
                months = copy.deepcopy(config.statics['all_months'])
                del months['all']
                months = sorted(months.keys())
            data_vars = ['m' + str(m) for m in months]

        for data_var in data_vars:
            m = self.get_month(t_res, data_var)
            d = int(config.statics['mon_len'][m - 1])
            dates_list.append(dt.datetime(yr, m, d))

        return dates_list

    def get_dbtable_datetime(self, year, t_res, data_var):
        '''
        :param t_res: temporal resolution
        :param data_var: data variable found in data files: for monthly m01, m02, ect.
        :return:
        '''
        yr = int(year)
        m = self.get_month(t_res, data_var)
        d = int(config.statics['mon_len'][m - 1])
        return dt.datetime(yr, m, d)

class query_Util(object):
    '''
    Class to support API queries
    '''
    def __init__(self, tv_vars, db_engine, session):
        self.tv_vars = tv_vars
        self.db_engine = db_engine
        self.session = session

        '''
        # Set up the db session
        Session = session_module.sessionmaker()
        Session.configure(bind=db_engine)
        self.session = Session()
        '''
    def start_session(self):
        # Set up the db session
        Session = session_module.sessionmaker()
        Session.configure(bind=self.db_engine)
        self.session = Session()

    def end_session(self):
        self.session.close()

    def check_query_params(self):
        '''
        Sanity checks on input tv_vars
        :return:
        '''
        pass

    def object_as_dict(self, obj):
        '''
        Converts single db query object to dict
        :param obj:
        :return: query dict
        '''
        return {c.key: getattr(obj, c.key)
                for c in inspect(obj).mapper.column_attrs}

    def check_if_data_in_db(self, geom_name):
        if self.session is None:
            session =  self.start_session()
        else:
            session = self.session
        data_query = session.query(Data).filter(
            Data.geom_name == geom_name,
            Data.year == int(self.tv_vars['year']),
            Data.dataset_id == config.statics['db_id_dataset'][self.tv_vars['dataset']],
            Data.temporal_resolution == self.tv_vars['temporal_resolution'],
            Data.variable_id == config.statics['db_id_variable'][self.tv_vars['variable']],
        )
        if self.session is None:
            self.end_session()

        if len(data_query.all()) != 0:
            return True
        else:
            return False


    def get_query_data(self):
        feature_index_list = [int(i) for i in self.tv_vars['feature_index_list']]
        rgn = self.tv_vars['region']
        # Set the dates list from temporal_resolution
        DU = date_Util()
        dates_list = DU.set_datetime_dates_list(self.tv_vars)

        # Set the geom_names from region and feature index
        geom_names = [rgn + '_' + str(f_idx) for f_idx in feature_index_list]
        rgn_id = config.statics['db_id_region'][rgn]
        '''
        # Not working
        data_query = self.session.query(Data).join(Geom).\
            filter(
                Geom.user_id == 0,
                Geom.region_id == rgn_id,
                Geom.name.in_(geom_names)
            ).\
            filter(
                Data.dataset_id == config.statics['db_id_dataset'][self.tv_vars['dataset']],
                Data.variable_id == config.statics['db_id_variable'][self.tv_vars['variable']],
                Data.temporal_resolution == self.tv_vars['temporal_resolution'],
                Data.data_date.in_(dates_list)
            )
        print('LOOOK')
        print(data_query)
        '''

        '''
        # Not Working
        data_query = self.session.query(Geom, Data). \
            filter(
            Geom.user_id == 0,
            Geom.region_id == rgn_id,
            Geom.name.in_(geom_names)
        ). \
            filter(
            Data.dataset_id == config.statics['db_id_dataset'][self.tv_vars['dataset']],
            Data.variable_id == config.statics['db_id_variable'][self.tv_vars['variable']],
            Data.temporal_resolution == self.tv_vars['temporal_resolution'],
            Data.data_date.in_(dates_list)
        )

        json_data = []
        for g, d in data_query.all():
            json_data.append(self.object_as_dict(d))
            # Convert datetime time stamp to datestring
            json_data[-1]['data_date'] = json_data[-1]['data_date'].strftime('%Y-%m-%d')
        '''


        # Working!
        # Query geometry table
        if len(feature_index_list) == 1 and feature_index_list[0] == 'all':
            geom_query = self.session.query(Geom).filter(
                Geom.user_id == 0,
                Geom.region_id == rgn_id
            )
        else:
            geom_query = self.session.query(Geom).filter(
                Geom.user_id == 0,
                Geom.region_id == rgn_id,
                Geom.feature_index.in_(feature_index_list)
            )
        # get the relevant geom_ids
        geom_id_list = [q.id for q in geom_query.all()]

        # Query data table
        data_query = self.session.query(Data).filter(
            Data.geom_id.in_(geom_id_list),
            Data.dataset_id == config.statics['db_id_dataset'][self.tv_vars['dataset']],
            Data.variable_id == config.statics['db_id_variable'][self.tv_vars['variable']],
            Data.temporal_resolution == self.tv_vars['temporal_resolution'],
            Data.data_date.in_(dates_list)
        )

        # Complile results as list of dicts
        json_data = []
        for q in data_query.all():
            json_data.append(self.object_as_dict(q))
            # Convert datetime time stamp to datestring
            json_data[-1]['data_date'] = json_data[-1]['data_date'].strftime('%Y-%m-%d')
        json_data = json.dumps(json_data, ensure_ascii=False).encode('utf8')
        return json_data
