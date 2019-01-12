import os
import datetime as dt
import logging
import json
import urllib2
import copy
import subprocess
import csv


import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import inspect
from shapely.geometry import asShape
from sqlalchemy import DDL
from sqlalchemy import event
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2.shape import from_shape, to_shape
from geoalchemy2.types import Geometry
import geojson

import config
class User(Base):
    __tablename__ = 'user'
    __table_args__ = {'schema': schema}
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String())
    email = db.Column(db.String())
    last_login = db.Column(db.DateTime())
    joined =  db.Column(db.DateTime())
    ip = db.Column(db.String())
    notes = db.Column(db.String())
    active = db.Column(db.String())
    role = db.Column(db.String())

    geometries = relationship('Geom', secondary=GeomUserLink, back_populates='users', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Model(Base):
    __tablename__ = 'model'
    __table_args__ = {'schema': schema}
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String())
    ee_collection = db.Column(db.String())
    model_collection = db.Column(db.String())

    data = relationship('Data', back_populates='model', cascade='save-update, merge, delete')
    parameters = relationship('Parameters', back_populates='model', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class ModelMetadata(Base):
    __tablename__ = 'model_metadata'
    __table_args__ = {'schema': schema}
    id = db.Column(db.Integer(), primary_key=True)
    model_name = db.Column(db.String(), db.ForeignKey(schema + '.' + 'model.name'), nullable=False)
    name = db.Column(db.String())
    properties = db.Column(db.String())

class Variable(Base):
    __tablename__ = 'variable'
    __table_args__ = {'schema': schema}
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String())
    units = db.Column(db.String())

    data = relationship('Data', back_populates='variable', cascade='save-update, merge, delete')
    parameters = relationship('Parameters', back_populates='variable', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


GeomUserLink = db.Table('geom_user_link', Base.metadata,
    db.Column('user_id', db.Integer, db.ForeignKey('user.id', ondelete='cascade', onupdate='cascade')),
    db.Column('geom_id', db.Integer, db.ForeignKey('geom.id', ondelete='cascade', onupdate='cascade'))
)

class Geom(Base):
    __tablename__ = 'geom'
    __table_args__ = {'schema': schema}
    id = db.Column(db.Integer(), primary_key=True)
    # user_id = db.Column(db.Integer(), db.ForeignKey(schema + '.' + 'user.id'), nullable=False)
    type = db.Column(db.String())
    year = db.Column(db.Integer())
    postgis_geom = db.Column(Geometry(geometry_type='MULTIPOLYGON'))

    metadata = relationship('GeomMetadata', back_populates='geom', cascade='save-update, merge, delete')
    data = relationship('Data', back_populates='geom', cascade='save-update, merge, delete')
    users = relationship('User', secondary=GeomUserLink, back_populates='geometries', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class GeomMetadata(Base):
    __tablename__ = 'geom_metadata'
    __table_args__ = {'schema': schema}
    id = db.Column(db.Integer(), primary_key=True)
    geom_id = db.Column(db.Integer(), db.ForeignKey(schema + '.' + 'geom.id'), nullable=False)
    name = db.Column(db.String())
    properties = db.Column(db.String())

    geom = relationship('Geom', back_populates='metadata', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Parameters(Base):
    __tablename__ = 'parameter'
    __table_args__ = {'schema': schema}
    id = db.Column(db.Integer(), primary_key=True)
    variable_name = db.Column(db.String(), db.ForeignKey(schema + '.'  + 'variable.name'), nullable=False)
    model_name = db.Column(db.String(), db.ForeignKey(schema + '.'  + 'model.name'), nullable=False)
    name =  db.Column(db.String())
    properties = db.Column(db.String())

    variable = relationship('Variable', back_populates='parameters', cascade='save-update, merge, delete')
    model = relationship('Model', back_populates='parameters', cascade='save-update, merge, delete')


    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Data(Base):
    __tablename__ = 'data'
    __table_args__ = {'schema': schema}
    id = db.Column(db.Integer(), primary_key=True)
    geom_id = db.Column(db.Integer(), db.ForeignKey(schema + '.' + 'geom.id'), nullable=False)
    model_name =  db.Column(db.String(), db.ForeignKey(schema + '.' + 'model.name'), nullable=False)
    variable_name =  db.Column(db.String(), db.ForeignKey(schema + '.' + 'variable.name'), nullable=False)
    temporal_resolution = db.Column(db.String())
    data_date = db.Column(db.DateTime())
    data_value = db.Column(db.Float(precision=4))

    geom = relationship('Geom', back_populates='data', cascade='save-update, merge, delete')
    model = relationship('Model', back_populates='data', cascade='save-update, merge, delete')
    variable = relationship('Variable', back_populates='data', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

#######################################
# OpenET database tables
#######################################
Base = declarative_base()
# schema='openet geodatabase'
schema = 'test'
# schema = 'public'
Base.metadata = db.MetaData(schema=schema)

event.listen(Base.metadata, 'before_create', DDL("CREATE SCHEMA IF NOT EXISTS " + schema))

'''
class Region(Base):
    # States, Counties, HUCs or fields or custom
    __tablename__ = 'region'
    __table_args__ = {'schema': schema}
    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String())
    geometries = relationship('Geom', back_populates='region', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
'''



#######################################
# END OpenET database tables
#######################################

class database_Util(object):
    '''
    Class to support database (cloudSQL) population
    Method:
        - The base query is defined from relevant template values
    Args:
        :region Unique ID of geojson file containing fields for the region
        :model SSEBop etc
        :year year of geojson model, might be ALL if not USFields
            USField geojsons change every year
    '''
    def __init__(self, region, model, year, user_id, region_changing_by_year):
        self.region = region
        self.year = int(year)
        self.model = model
        self.user_id = user_id
        self.geo_bucket_url = config.GEO_BUCKET_URL
        self.data_bucket_url = config.DATA_BUCKET_URL
        self.region_changing_by_year = region_changing_by_year

        # Used to read geometry data from buckets
        if self.region_changing_by_year:
            # Field boundaries depend on years
            self.geoFName = region + '_' + str(year) + '.geojson'
        else:
            self.geoFName = region + '.geojson'
        self.dataFName = region + '_' + str(year) + '_DATA'  '.json'

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
        url = self.data_bucket_url + self.model + '/' + self.dataFName
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

    def add_in_chunks(self, entity_list, session):
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

            session.add_all(entities)
            try:
                session.commit()
            except:
                session.rollback()
                raise
            num_added = end

    def check_if_data_in_db(self, geom_id, session):
        # Check if this entry is already in db
        in_db =  False
        QU = query_Util({
            'region': self.region,
            'model': self.model,
            'year': self.year,
            'temporal_resolution': 'monthly',
            'variable': 'et'
        }, session)
        in_db = QU.check_if_data_in_db(geom_id)
        return in_db

    def check_if_geom_in_db(self, region, feature_index, year, session):
        geom_query = session.query(Geom, GeomMetadata).filter(
            GeomMetadata.name == "feature_index",
            GeomMetadata.properties == feature_index

        ).filter(
            GeomMetadata.name == "collection_id",
            GeomMetadata.properties == region
        ).filter(
            Geom.year == year
        ).filter(
            Geom.id == GeomMetadata.geom_id
        )

        if len(geom_query.all()) == 0:
            return None, None
        if len(geom_query.all()) > 1:
            logging.error('Multiple geometries for ' + region + '/' + str(feature_index) + '/' + str(year))
            return -9999, None
        geom = geom_query.first()
        geom_id = geom.id
        return geom_id

    def set_postgis_geometry(self, shapely_geom):
        postgis_geom = None
        if shapely_geom.geom_type == 'Polygon':
            # convert to multi polygon
            postgis_geom = from_shape(MultiPolygon([shapely_geom]))
        elif shapely_geom.geom_type == 'MultiPolygon':
            # Save as is
            postgis_geom = from_shape(shapely_geom)
        return postgis_geom

    def set_geom_entity(self, geom_type, postgis_geometry, year):
        '''
        Adds the geometry row to database and retrieves the automatically
        assigned primary key geom_id
        # Note: primary key is AUTOSET in db
        '''
        geometry = Geom(
            # user_id = self.user_id,
            type = geom_type,
            year=int(year),
            postgis_geom=postgis_geometry
        )
        return geometry

    def add_entities_to_db(self, session, entities):
        session.add_all(entities)
        try:
            session.commit()
        except:
            session.rollback()
            raise


    def set_database_tables(self, session):
        # User
        entities = []
        for user_id in config.statics['users'].keys():
            init_dict = config.statics['users'][user_id]
            init_dict['last_login'] = dt.datetime.today()
            init_dict['joined'] = dt.datetime.today()

            entities.append(User(**init_dict))
        self.add_entities_to_db(session, entities)
        print('Added User Table')

        # Model
        entities = []
        for model_name in config.statics['models'].keys():
            init_dict = config.statics['models'][model_name]
            # This goes into the parameter table
            del init_dict['parameters']
            # This goes into the model_metdata table
            del init_dict['metadata']
            entities.append(Model(**init_dict))
        self.add_entities_to_db(session, entities)
        print('Added Model Table')

        # ModelMetdata
        entities = []
        for model_name in config.statics['models'].keys():
            init_dict = config.statics['models'][model_name]['metadata']
            entities.append(ModelMetadata(**init_dict))
        self.add_entities_to_db(session, entities)
        print('Added ModelMetadata Table')

        # Variable
        entities = []
        for var_name in config.statics['variables'].keys():
            init_dict = config.statics['variables'][var_name]
            entities.append(Variable(**init_dict))
        self.add_entities_to_db(session, entities)
        print('Added Variable rows')

        # Parameters (depends on model AND variable)
        entities = []
        for model_name in config.statics['parameters'].keys():
            for var_name in config.statics['parameters'][model_name].keys():
                init_dict = config.statics['parameters'][model_name][var_name]
                entities.append(Parameters(**init_dict))
        self.add_entities_to_db(session, entities)
        print('Added Parameter rows')

        # NOTE: Geom, GeomMetdata tables are set later


    def add_data_to_db(self, session, user_id=0, etdata=None, geojson_data=None):
        '''
        Add data to database
        :return:
        '''
        # Read etdata from bucket
        if etdata is None:
            etdata = self.read_etdata_from_bucket()

        if geojson_data is None:
            geojson_data = self.read_geodata_from_bucket()

        # Set the user ids associated with this region
        user_ids_for_geom = config.statics['regions'][self.region]['users']

        # Check if database is empty
        # If not empty, we need to check if entries are already in db
        db_empty = False
        q = session.query(Data).first()
        if q is None:
            db_empty = True
        if db_empty:
            # Set up region, model, parameter and variable tables
            print('Database empty, setting up basic data tables')
            self.set_database_tables(session)

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
        conn = session.connection()  # SQLAlchemy Connection
        dbapi_conn = conn.connection  # DBAPI connection (technically a connection pool wrapper called ConnectionFairy, but everything is there)
        cursor = dbapi_conn.cursor()  # actual DBAPI cursor
        cursor.execute("SET search_path TO myschema," + schema + ', public')

        while chunk <= num_chunks:
            '''
            data_entities = []
            meta_entities = []
            '''
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
                '''
                # Find the geometry name
                geom_name = 'Not found'
                for name_prop in config.statics['geom_name_keys']:
                    try:
                        geom_name = geojson_data['features'][f_idx]['properties'][name_prop]
                        geom_name = geom_name.encode('utf-8').strip()
                        break
                    except:
                        continue
                '''
                print('Adding Feature '  + str(f_idx + 1))

                # Geometry data table
                # check if the geometry is already in the database
                if self.region_changing_by_year:
                    year = self.year
                else:
                    year = 9999
                g_data = geojson_data['features'][f_idx]
                geom_id, geom_area = self.check_if_geom_in_db(self.region, feat_idx, year, session)
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
                    geometry = self.set_geom_entity(feat_idx, shapely_geom.geom_type, postgis_geom, year)
                    # Submit the geom table to obtain the primary key geom_id
                    self.add_entities_to_db(self, session, [geometry])
                    # Get the geometry primary key from db
                    geom_id = geometry.id
                    logging.info('Added Geometry Table')
                    # Add the many-to-many relationship between user and geom
                    # (user_id, geom_id pairs)
                    uid_geomid_pairs = []
                    for user_id in user_ids_for_geom:
                        uid_geomid_pairs.append((user_id, geom_id))
                    session.execute(GeomUserLink.insert().values(uid_geomid_pairs))
                    print('Added GeomUserLink Table')
                else:
                    logging.info('Geometry found in db')
                    print('Geometry found in db')

                # Check if the data is in db
                in_db = self.check_if_data_in_db(geom_id, session)
                if in_db:
                    print('Data for geom_id/year ' + str(geom_id) + '/' + str(self.year) + ' found in db. Skipping...')
                    continue

                f_data = etdata['features'][f_idx]
                # Set the geometry metadata and data tables for bulk ingest
                for key in config.statics['regions'][self.region]['metadata']:
                    try:
                        value = str(g_data['properties'][key])
                    except:
                        try:
                           value = str(f_data['properties'][key])
                        except:
                            value = 'Not Found'
                    # Remove commas, causes issues when copy_from
                    value = ' '.join(value.replace(', ', ',').split(','))
                    '''
                    init_dict = {'geom_id': geom_id, 'key': key, 'value': value}
                    meta_entities.append(self.set_geom_metadata_entity(init_dict))
                    '''
                    csv_mwriter.writerow([geom_id, key, value])


                # Variable loop
                for var in config.statics['models'][self.model]['variables']:
                    for t_res in config.statics['temporal_resolution'].keys():
                        for data_var in config.statics['temporal_resoution'][t_res]['data_vars']:
                            # Set date
                            DU = date_Util()
                            data_date = DU.get_dbtable_datetime(self.year, t_res, data_var)
                            # Set data value
                            try:
                                data_value = float(f_data['properties'][var + '_' + data_var])
                            except:
                                data_value = -9999
                            '''
                            init_dict = {
                                'geom_id': geom_id,
                                'model_name': self.model,
                                'variable_name': var,
                                'temporal_resolution': t_res,
                                'data_date': data_date,
                                'data_value': data_value
                            }
                            data_entities.append(self.set_data_entity(init_dict))
                            '''
                            row = [geom_id, self.model, var, t_res, data_date, data_value]
                            csv_dwriter.writerow(row)

            csv_metadata.close()
            csv_data.close()

            # Commit the geom metadata and data for all features
            with open('data.csv', 'r') as f:
                if os.stat("data.csv").st_size != 0:
                    cols = ('geom_id', 'model_name', 'variable_name',
                            'temporal_resolution', 'data_date', 'data_value')
                    cursor.copy_from(f, 'data', sep=',', columns=cols)
                    print('Added Data tables for features')

            with open('metadata.csv', 'r') as f:
                if os.stat("metadata.csv").st_size != 0:
                    cols = ('geom_id', 'name', 'properties')
                    cursor.copy_from(f, 'geom_metadata', sep=',', columns=cols)
                    print('Added GeomMetadata table rows for features')
            try:
                session.commit()
            except:
                session.rollback()
                raise


            os.remove('metadata.csv')
            os.remove('data.csv')
            chunk += 1
        # Close the connection
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
    def __init__(self, tv_vars, session):
        self.tv_vars = tv_vars
        self.session = session

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

    def check_if_data_in_db(self, geom_id):
        data_query = self.session.query(Data).filter(
            Data.geom_id == int(geom_id),
            Data.year == int(self.tv_vars['year']),
            Data.model_name == self.tv_vars['model'],
            Data.temporal_resolution == self.tv_vars['temporal_resolution'],
            Data.variable_name == self.tv_vars['variable']
        )
        if len(data_query.all()) != 0:
            return True
        else:
            return False


    def get_query_data(self):
        if 'feature_index_list' in self.tv_vars.keys():
            feature_index_list = [int(i) for i in self.tv_vars['feature_index_list']]
        else:
            feature_index_list = ['all']
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
                Data.model_id == config.statics['db_id_model'][self.tv_vars['model']],
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
            Geom.region_id == rgn_id,
            Geom.name.in_(geom_names)
        ). \
            filter(
            Data.model_id == config.statics['db_id_model'][self.tv_vars['model']],
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
            '''
            geom_query = self.session.query(Geom).filter(
                Geom.user_id == 0,
                Geom.region_id == rgn_id,
                Geom.feature_index.in_(feature_index_list)
            )
            '''
            geom_query = self.session.query(Geom).filter(Geom.users.any(id=0))
            # geom_query = self.session.query(Geom).join(Geom.users).filter_by(id=0)



        # get the relevant geom_ids
        geom_id_list = [q.id for q in geom_query.all()]

        # Query data table
        data_query = self.session.query(Data).filter(
            Data.geom_id.in_(geom_id_list),
            Data.model_id == config.statics['db_id_model'][self.tv_vars['model']],
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
