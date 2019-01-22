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

#######################################
# OpenET database tables
#######################################
Base = declarative_base()
# schema='openet geodatabase'
schema = config.schema
Base.metadata = db.MetaData(schema=schema)

#event.listen(Base.metadata, 'before_create', DDL('CREATE SCHEMA IF NOT EXISTS ' + schema))

# FIXME: mssing tables: Report, Parameters, commented out below
class Model(Base):
    __tablename__ = 'model'
    __table_args__ = {'schema': schema}
    model_id = db.Column(db.Integer(), primary_key=True)
    model_name = db.Column(db.String(), unique=True, index=True)
    ee_collection_name = db.Column(db.String())
    model_collection = db.Column(db.String())

    data = relationship('Data', back_populates='model', cascade='save-update, merge, delete')
    model_metadata = relationship('ModelMetadata', back_populates='model', cascade='save-update, merge, delete')
    # parameters = relationship('Parameters', back_populates='model', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class ModelMetadata(Base):
    __tablename__ = 'model_metadata'
    __table_args__ = {'schema': schema}
    model_metadata_id = db.Column(db.Integer(), primary_key=True)
    model_name = db.Column(db.String(), db.ForeignKey(schema + '.' + 'model.model_name'), index=True, nullable=False)
    model_metadata_name = db.Column(db.String())
    model_metadata_properties = db.Column(db.String())

    model = relationship('Model', back_populates='model_metadata', cascade='save-update, merge, delete')


FeatureUserLink = db.Table('feature_user_link', Base.metadata,
    # db.Column('feature_collection_id', db.Integer, db.ForeignKey('feature_collection.feature_collection_id', ondelete='cascade', onupdate='cascade')),
    db.Column('user_id', db.Integer, db.ForeignKey('user.user_id', ondelete='cascade', onupdate='cascade')),
    db.Column('feature_id', db.Integer, db.ForeignKey('feature.feature_id', ondelete='cascade', onupdate='cascade'))
)

class User(Base):
    __tablename__ = 'user'
    __table_args__ = {'schema': schema}
    user_id = db.Column(db.Integer(), primary_key=True)
    user_name = db.Column(db.String())
    user_email = db.Column(db.String())
    last_login = db.Column(db.DateTime())
    joined =  db.Column(db.DateTime())
    ip = db.Column(db.String())
    notes = db.Column(db.String())
    active = db.Column(db.String())
    role = db.Column(db.String())


    features = relationship('Feature', secondary=FeatureUserLink, back_populates='users', cascade='save-update, merge, delete')
    data = relationship('Data', back_populates='user', cascade='save-update, merge, delete')
    feature_colections = relationship('FeatureCollection', back_populates='users', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class FeatureCollection(Base):
    __tablename__ = 'feature_collection'
    __table_args__ = {'schema': schema}
    feature_collection_id = db.Column(db.Integer(), primary_key=True)
    feature_collection_name =  db.Column(db.String(), unique=True, nullable=False)
    user_id =  db.Column(db.Integer(), db.ForeignKey(schema + '.' + 'user.user_id'), nullable=False)
    feature_collection_permission = db.Column(db.String())
    url_path_to_shapefile = db.Column(db.String())

    users = relationship('User', back_populates='feature_colections',cascade='save-update, merge, delete')


class Feature(Base):
    __tablename__ = 'feature'
    __table_args__ = {'schema': schema}
    feature_id = db.Column(db.Integer(), primary_key=True)
    feature_collection_name = db.Column(db.String(), db.ForeignKey(schema + '.' + 'feature_collection.feature_collection_name'), nullable=False)
    feature_id_from_user = db.Column(db.String())
    type = db.Column(db.String())
    year = db.Column(db.Integer())
    geometry = db.Column(Geometry(geometry_type='MULTIPOLYGON'))

    feature_metadata = relationship('FeatureMetadata', back_populates='feature', cascade='save-update, merge, delete')
    data = relationship('Data', back_populates='feature', cascade='save-update, merge, delete')
    users = relationship('User', secondary=FeatureUserLink, back_populates='features', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class FeatureMetadata(Base):
    __tablename__ = 'feature_metadata'
    __table_args__ = {'schema': schema}
    feature_metadata_id = db.Column(db.Integer(), primary_key=True)
    feature_id = db.Column(db.Integer(), db.ForeignKey(schema + '.' + 'feature.feature_id'), nullable=False)
    feature_metadata_name = db.Column(db.String())
    feature_metadata_properties = db.Column(db.String())

    feature = relationship('Feature', back_populates='feature_metadata', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Variable(Base):
    __tablename__ = 'variable'
    __table_args__ = {'schema': schema}
    variable_id = db.Column(db.Integer(), primary_key=True)
    variable_name = db.Column(db.String(), unique=True, index=True)
    units = db.Column(db.String())

    data = relationship('Data', back_populates='variable', cascade='save-update, merge, delete')
    # parameters = relationship('Parameters', back_populates='variable', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Data(Base):
    __tablename__ = 'data'
    __table_args__ = {'schema': schema}
    data_id = db.Column(db.Integer(), primary_key=True)
    feature_id = db.Column(db.Integer(), db.ForeignKey(schema + '.' + 'feature.feature_id'), nullable=False)
    user_id =  db.Column(db.Integer(), db.ForeignKey(schema + '.' + 'user.user_id'), nullable=False)
    timeseries_id = db.Column(db.Integer(), db.ForeignKey(schema + '.' + 'timeseries.timeseries_id'), nullable=False)
    # report_id  = db.Column(db.Integer(), db.ForeignKey(schema + '.' + 'report.report_id'))
    model_name =  db.Column(db.String(), db.ForeignKey(schema + '.' + 'model.model_name'), index=True, nullable=False)
    variable_name =  db.Column(db.String(), db.ForeignKey(schema + '.' + 'variable.variable_name'), index=True, nullable=False)
    temporal_resolution = db.Column(db.String())
    permission = db.Column(db.String())
    last_timeseries_update = db.Column(db.DateTime())

    feature = relationship('Feature', back_populates='data', cascade='save-update, merge, delete')
    user = relationship('User', back_populates='data', cascade='save-update, merge, delete')
    timeseries = relationship('Timeseries', back_populates='data', cascade='save-update, merge, delete')
    # report = relationship('Report', back_populates='data', cascade='save-update, merge, delete')
    model = relationship('Model', back_populates='data', cascade='save-update, merge, delete')
    variable = relationship('Variable', back_populates='data', cascade='save-update, merge, delete')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Timeseries(Base):
    __tablename__ = 'timeseries'
    __table_args__ = {'schema': schema}
    timeseries_id = db.Column(db.Integer(), primary_key=True)
    start_date = db.Column(db.DateTime())
    end_date = db.Column(db.DateTime())
    data_value = db.Column(db.Float(precision=4))

    data = relationship('Data', back_populates='timeseries', cascade='save-update, merge, delete')

'''
class Parameters(Base):
    __tablename__ = 'parameter'
    __table_args__ = {'schema': schema}
    parameter_id = db.Column(db.Integer(), primary_key=True)
    variable_name = db.Column(db.String(), db.ForeignKey(schema + '.'  + 'variable.variable_name'), index=True, nullable=False)
    model_name = db.Column(db.String(), db.ForeignKey(schema + '.'  + 'model.model_name'), index=True, nullable=False)
    parameter_name =  db.Column(db.String())
    parameter_properties = db.Column(db.String())

    variable = relationship('Variable', back_populates='parameters', cascade='save-update, merge, delete')
    model = relationship('Model', back_populates='parameters', cascade='save-update, merge, delete')


    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Report(Base):
    __tablename__ = 'report'
    __table_args__ = {'schema': schema}
    report_id = db.Column(db.Integer(), primary_key=True)
'''

#######################################
# END OpenET database Utility class
#######################################

class database_Util(object):
    '''
    Class to support database (cloudSQL) population
    Method:
        - The base query is defined from relevant template values
    Args:
        :feature_collection Unique ID of geojson file containing fields for the feature_collection
        :model SSEBop etc
        :year year of geojson model, might be ALL if not USFields
            USField geojsons change every year
    '''
    def __init__(self, feature_collection, model, year, user_id, feature_collection_changing_by_year, engine):
        self.feature_collection = feature_collection
        self.year = int(year)
        self.model = model
        self.user_id = user_id
        self.geo_bucket_url = config.GEO_BUCKET_URL
        self.data_bucket_url = config.DATA_BUCKET_URL
        self.feature_collection_changing_by_year = feature_collection_changing_by_year
        self.engine = engine

        # Used to read geometry data from buckets
        if self.feature_collection_changing_by_year:
            # Field boundaries depend on years
            self.geoFName = feature_collection + '_' + str(year) + '.geojson'
        else:
            self.geoFName = feature_collection + '.geojson'
        self.dataFName = feature_collection + '_' + str(year) + '.json'

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
        d = json.load(urllib2.urlopen(url))
        print('Reading data from bucket file ' + url)
        '''
        try:
            d = json.load(urllib2.urlopen(url))
            print('Read data from bucket file ' + url)
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

    def database_is_empty(self):
        table_names = db.inspect(self.engine).get_table_names()
        is_empty = table_names == []
        print('Db is empty: {}'.format(is_empty))
        return is_empty

    def table_exists(self, table_name):
        ret = self.engine.dialect.has_table(self.engine, table_name)
        print('Table "{}" exists: {}'.format(table_name, ret))
        return ret

    def check_if_data_in_db(self, feature_id, year, session):
        # Check if this entry is already in db
        in_db =  False
        if feature_id is None:
            return in_db

        QU = query_Util({
            'feature_collection': self.feature_collection,
            'model': self.model,
            'year': year,
            'temporal_resolution': 'monthly',
            'variable': 'et'
        }, session)
        in_db = QU.check_if_data_in_db(feature_id)
        return in_db


    def check_if_feature_in_db(self, feature_collection, feature_id_from_user, year, session):
        coll_name = config.statics['feature_collections'][feature_collection]['feature_collection_name']
        feature_query = session.query(Feature).filter(
            Feature.feature_id_from_user == str(feature_id_from_user),
            Feature.year == year,
            Feature.feature_collection_name == coll_name
        )
        if len(feature_query.all()) == 0:
            return None
        if len(feature_query.all()) > 1:
            logging.error('Multiple geometries for ' + feature_collection + '/' + str(feature_id_from_user) + '/' + str(year))
            return None
        feature = feature_query.first()
        feature_id = feature.feature_id
        return feature_id

    def set_postgis_geometry(self, shapely_geom):
        postgis_geom = None
        if shapely_geom.geom_type == 'Polygon':
            # convert to multi polygon
            postgis_geom = from_shape(MultiPolygon([shapely_geom]))
        elif shapely_geom.geom_type == 'MultiPolygon':
            # Save as is
            postgis_geom = from_shape(shapely_geom)
        return postgis_geom

    def set_feature_entity(self, feature_id_from_user, geom_type, postgis_geometry, year):
        '''
        Adds the geometry row to database and retrieves the automatically
        assigned primary key geom_id
        # Note: primary key is AUTOSET in db
                feature_id_from user is set to feature_index in featCollection if user didn't give it
        '''
        coll_name = config.statics['feature_collections'][self.feature_collection]['feature_collection_name']
        feature = Feature(
            feature_collection_name = coll_name,
            feature_id_from_user = feature_id_from_user,
            type = geom_type,
            year = int(year),
            geometry = postgis_geometry
        )
        return feature

    def add_entity_to_db(self, session, entity):
        '''
        Add single entity to db
        :param session:
        :param entities:
        :return:
        '''
        session.add(entity)
        try:
            session.commit()
        except:
            session.rollback()
            raise

    def add_entities_to_db(self, session, entities):
        '''
        Add multiple entities to db
        :param session:
        :param entities:
        :return:
        '''
        session.add_all(entities)
        try:
            session.commit()
        except:
            session.rollback()
            raise


    def set_user_dict(self, user_id):
        '''
        set the dictonary used to populate db User table  for a single user
        so that adding users is simple
        Note: user parameters need to be stored in config.statics['user']
        :param user_id:
        :return:
        '''
        init_dict = copy.deepcopy(config.statics['users'][user_id])
        # Update date parameters
        init_dict['last_login'] = dt.datetime.today()
        init_dict['joined'] = dt.datetime.today()
        return init_dict

    def add_user_to_db(self, init_dict, session):
        entity = User(**init_dict)
        self.add_entity_to_db(session, entity)

    def set_base_database_tables(self, session):
        # User
        entities = []
        for user_id in config.statics['users'].keys():
            init_dict = copy.deepcopy(self.set_user_dict(user_id))
            entities.append(User(**init_dict))
        self.add_entities_to_db(session, entities)
        print('Added User Table')

        # Model ModelMetadata
        entities = []
        m_entities = []
        for model_name in config.statics['models'].keys():
            init_dict = copy.deepcopy(config.statics['models'][model_name])
            # This goes into the model_metdata table
            del init_dict['variables']
            del init_dict['metadata']
            entities.append(Model(**init_dict))

            init_dict = copy.deepcopy(config.statics['models'][model_name]['metadata'])
            init_dict['model_name'] = model_name
            m_entities.append(ModelMetadata(**init_dict))


        self.add_entities_to_db(session, entities)
        self.add_entities_to_db(session, m_entities)
        del m_entities
        print('Added Model and ModelMetadata Tables')

        # Variable
        entities = []
        for var_name in config.statics['variables'].keys():
            init_dict = copy.deepcopy(config.statics['variables'][var_name])
            entities.append(Variable(**init_dict))
        self.add_entities_to_db(session, entities)
        print('Added Variable Table')

        # FeatureCollection
        entities = []
        for coll_name in config.statics['feature_collections'].keys():
            coll_dict = copy.deepcopy(config.statics['feature_collections'][coll_name])
            del coll_dict['metadata']
            users = coll_dict['users']
            del coll_dict['users']
            for user in users:
                init_dict = copy.deepcopy(coll_dict)
                init_dict['user_id'] = user
                entities.append(FeatureCollection(**init_dict))
        self.add_entities_to_db(session, entities)
        print('Added FeatureCollection table')

        '''
        # Parameters (depends on model AND variable)
        entities = []
        for model_name in config.statics['parameters'].keys():
            for var_name in config.statics['parameters'][model_name].keys():
                init_dict = copy.deepcopy(config.statics['parameters'][model_name][var_name])
                entities.append(Parameters(**init_dict))
        self.add_entities_to_db(session, entities)
        print('Added Parameter rows')
        '''
        # NOTE: Feature, FeatureMetdata tables are set later

    def add_data_to_db(self, session, user_id=0, etdata=None, geojson_data=None):
        '''
        Add data to database
        :params:
            session: database session
            user_id
            etdata: json object containing the data, if None, data is read from bucket
            geojson_data: contains the geometry information as geojson, if None, data is read from bucket
        :return:
        '''
        # Read etdata from bucket
        if etdata is None:
            etdata = self.read_etdata_from_bucket()

        if geojson_data is None:
            geojson_data = self.read_geodata_from_bucket()

        # Set the user ids associated with this feature_collection
        user_ids_for_featColl = config.statics['feature_collections'][self.feature_collection]['users']

        # Check if database is empty
        # If not empty, we need to check if entries are already in db
        db_empty = False

        q = session.query(Data).first()
        if q is None:
            db_empty = True

        # db_empty = self.database_is_empty()

        if db_empty:
            # Set up feature_collection, model, parameter and variable tables
            print('Database empty, setting up basic data tables')
            self.set_base_database_tables(session)


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

        # FIXME: these should not be hardcoded here
        permission = 'public'
        last_timeseries_update = dt.datetime.today()
        report_id = 0
        timeseries_id = 0
        while chunk <= num_chunks:
            '''
            data_entities = []
            meta_entities = []
            '''
            csv_metadata = open('metadata.csv', 'wb+')
            csv_data = open('data.csv', 'wb+')
            csv_timeseries = open('timeseries.csv', 'wb+')
            csv_meta_writer = csv.writer(csv_metadata, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            csv_data_writer = csv.writer(csv_data, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            csv_ts_writer = csv.writer(csv_timeseries, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

            idx_start = (chunk - 1) * chunk_size
            idx_end = chunk * chunk_size
            if idx_end > len(etdata['features']):
                idx_end = len(etdata['features'])

            for f_idx in range(idx_start, idx_end):
                feat_idx = f_idx +1
                print('Adding Feature '  + str(f_idx + 1))
                # Feature table
                # check if the feature is already in the database
                if self.feature_collection_changing_by_year:
                    year = self.year
                else:
                    year = 9999
                g_data = geojson_data['features'][f_idx]
                # Set the feature_id_from_user
                if 'feature_id_from_user' in g_data['properties'].keys():
                    feature_id_from_user = str(g_data['properties']["feature_id_from_user"])
                else:
                    feature_id_from_user = str(feat_idx)

                # Check if feature is in db
                feature_id = self.check_if_feature_in_db(self.feature_collection, str(feat_idx), year, session)
                # Check if  data is in db
                data_in_db = self.check_if_data_in_db(feature_id, self.year, session)

                if feature_id and data_in_db:
                    print('Data for feature_id/year ' + str(feature_id) + '/' + str(self.year) + ' found in db. Skipping...')
                    continue

                if not feature_id:
                    # Convert the geojson geometry to postgis geometry using shapely
                    # Note: we convert polygons to multi polygon
                    # Convert to shapely shape
                    shapely_geom = asShape(g_data['geometry'])
                    postgis_geom = self.set_postgis_geometry(shapely_geom)
                    if postgis_geom is None:
                        raise Exception('Not a valid geometry, must be polygon or multi polygon!')
                    # Add the feature table entry for this feature and obtain the feature_id
                    feature = self.set_feature_entity(feature_id_from_user, shapely_geom.geom_type, postgis_geom, year)
                    # Submit the feature table to obtain the primary key feature_id
                    self.add_entity_to_db(session, feature)
                    # Get the feature primary key from db
                    feature_id = feature.feature_id
                    logging.info('Added Feature Table')
                    # Add the many-to-many relationship between user and feature
                    # (user_id, feature_id pairs)
                    uid_feat_pairs = []
                    for user_id in user_ids_for_featColl:
                        uid_feat_pairs.append((user_id, feature_id))
                else:
                    logging.info('Feature found in db')
                    print('Feature found in db')

                f_data = etdata['features'][f_idx]
                # Set the feature metadata and data tables for bulk ingest
                for key in config.statics['feature_collections'][self.feature_collection]['metadata']:
                    try:
                        value = str(g_data['properties'][key])
                    except:
                        try:
                           value = str(f_data['properties'][key])
                        except:
                            value = 'Not Found'
                    # Remove commas, causes issues when copy_from
                    value = ' '.join(value.replace(', ', ',').split(','))
                    csv_meta_writer.writerow([feature_id, key, value])

                # Variable loop
                for var in config.statics['models'][self.model]['variables']:
                    for t_res in config.statics['temporal_resolution'].keys():
                        for data_var in config.statics['temporal_resolution'][t_res]['data_vars']:
                            timeseries_id+=1
                            # Set date
                            DU = date_Util()
                            start_date_dt, end_date_dt = DU.get_dbtable_start_end_dates(self.year, t_res, data_var)
                            # Set data value
                            try:
                                data_value = float(f_data['properties'][var + '_' + data_var])
                            except:
                                data_value = -9999

                            row = [timeseries_id, start_date_dt, end_date_dt, data_value]
                            csv_ts_writer.writerow(row)
                            # row = [feature_id, user_id, timeseries_id, report_id, self.model, var, t_res, permission, last_timeseries_update]
                            row = [feature_id, user_id, timeseries_id, self.model, var, t_res, permission, last_timeseries_update]
                            csv_data_writer.writerow(row)


            session.execute(FeatureUserLink.insert().values(uid_feat_pairs))
            print('Added FeatureUserLink Table')

            csv_metadata.close()
            csv_timeseries.close()
            csv_data.close()


            # Commit the feature metadata and data for all features
            with open('timeseries.csv', 'r') as f:
                if os.stat("timeseries.csv").st_size != 0:
                    cols = ('timeseries_id', 'start_date', 'end_date', 'data_value')
                    cursor.copy_from(f, 'timeseries', sep=',', columns=cols)
                    print('Added timeseries table rows for features')

            with open('data.csv', 'r') as f:
                if os.stat("data.csv").st_size != 0:
                    cols = ('feature_id', 'user_id', 'timeseries_id',
                            'model_name', 'variable_name', 'temporal_resolution',
                            'permission', 'last_timeseries_update')
                    cursor.copy_from(f, 'data', sep=',', columns=cols)
                    print('Added Data tables for features')

            with open('metadata.csv', 'r') as f:
                if os.stat("metadata.csv").st_size != 0:
                    cols = ('feature_id', 'feature_metadata_name', 'feature_metadata_properties')
                    cursor.copy_from(f, 'feature_metadata', sep=',', columns=cols)
                    print('Added FeatureMetadata table rows for features')



            try:
                session.commit()
            except:
                session.rollback()
                raise

            # Delete the csv files
            os.remove('metadata.csv')
            os.remove('data.csv')
            os.remove('timeseries.csv')
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
            dates_list.append([dt.datetime(yr, m, 1), dt.datetime(yr, m, d)])

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

    def get_dbtable_start_end_dates(self, year, t_res, data_var):
        '''
        :param t_res: temporal resolution
        :param data_var: data variable found in data files: for monthly m01, m02, ect.
        :return:
        '''
        yr = int(year)
        m = self.get_month(t_res, data_var)
        d = int(config.statics['mon_len'][m - 1])
        start_date = dt.datetime(yr, m, 1)
        end_date = dt.datetime(yr, m, d)
        return start_date, end_date

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

    def check_if_data_in_db(self, feature_id):
        data_query = self.session.query(Data).filter(
            Data.feature_id == int(feature_id),
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
        feat_coll = self.tv_vars['feature_collection']
        # Set the dates list from temporal_resolution
        DU = date_Util()
        dates_list = zip(DU.set_datetime_dates_list(self.tv_vars))
        start_dates = dates_list[0]
        end_dates = dates_list[1]

        # Set the geom_names from feature_collection and feature index
        feat_coll_name = config.statics['regions'][self.region]['feature_collection_name']
        # FIXME: user joins!!!
        # Query geometry table
        if len(feature_index_list) == 1 and feature_index_list[0] == 'all':
            feature_query = self.session.query(Feature).filter(
                Feature.user_id == 0,
                Feature.feature_collection_name == feat_coll_name
            )
        else:
            feature_query = self.session.query(Feature).filter(Feature.users.any(id=0))
            # feature_query = self.session.query(Feature).join(Feature.users).filter_by(id=0)


        # get the relevant geom_ids
        feature_id_list = [q.id for q in feature_query.all()]

        # Query data table
        data_query = self.session.query(Data, Timeseries).filter(
            Data.geom_id.in_(feature_id_list),
            Data.model_id == config.statics['db_id_model'][self.tv_vars['model']],
            Data.variable_id == config.statics['db_id_variable'][self.tv_vars['variable']],
            Data.temporal_resolution == self.tv_vars['temporal_resolution'],
        ).filter(
            Timeseries.start_date.in_(start_dates),
            Timeseries.start_date.in_(end_dates)
        )

        # Complile results as list of dicts
        json_data = []
        for q in data_query.all():
            json_data.append(self.object_as_dict(q))
            # Convert datetime time stamp to datestring
            json_data[-1]['data_date'] = json_data[-1]['data_date'].strftime('%Y-%m-%d')
        json_data = json.dumps(json_data, ensure_ascii=False).encode('utf8')
        return json_data
