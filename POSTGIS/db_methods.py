import os
import datetime as dt
import logging
import json
import urllib2
import copy
import subprocess
import csv


import sqlalchemy as db
from sqlalchemy.orm import session as session_module
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import inspect
from shapely.geometry import asShape
from sqlalchemy import DDL
from sqlalchemy import event
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2.shape import from_shape, to_shape
from geoalchemy2.types import Geometry

import sqlalchemy.sql as sqa

'''
from sqlalchemy.sql import expression as expr
# from sqlalchemy.sql import select, and_, or_, not_
from sqlalchemy.sql import func, asc, desc, text
'''

import geojson
import numpy as np
import config


from populate_db import SCHEMA as schema
from populate_db import GEO_BUCKET_URL, DATA_BUCKET_URL

#######################################
# OpenET database tables
#######################################
Base = declarative_base()
# schema = config.NASA_ROSES_SCHEMA

Base.metadata = db.MetaData(schema=schema)

#event.listen(Base.metadata, "before_create", DDL("CREATE SCHEMA IF NOT EXISTS " + schema))

# Many-to_many
FeatureUserLink = db.Table("feature_user_link", Base.metadata,
    db.Column("user_id", db.Integer, db.ForeignKey("user.user_id", ondelete="cascade", onupdate="cascade")),
    db.Column("feature_id", db.Integer, db.ForeignKey("feature.feature_id", ondelete="cascade", onupdate="cascade"))
)

# FIXME: mssing tables: Report, Parameters, commented out below
class Model(Base):
    __tablename__ = "model"
    __table_args__ = {"schema": schema}
    model_id = db.Column(db.Integer(), primary_key=True)
    model_name = db.Column(db.String(), unique=True, index=True)
    ee_collection_name = db.Column(db.String())
    model_collection = db.Column(db.String())

    data = relationship("Data", back_populates="model", cascade="save-update, merge, delete")
    model_metadata = relationship("ModelMetadata", back_populates="model", cascade="save-update, merge, delete")
    # parameters = relationship("Parameters", back_populates="model", cascade="save-update, merge, delete")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class ModelMetadata(Base):
    __tablename__ = "model_metadata"
    __table_args__ = {"schema": schema}
    model_metadata_id = db.Column(db.Integer(), primary_key=True)
    model_name = db.Column(db.String(), db.ForeignKey(schema + "." + "model.model_name"), index=True, nullable=False)
    model_metadata_name = db.Column(db.String())
    model_metadata_properties = db.Column(db.String())

    model = relationship("Model", back_populates="model_metadata", cascade="save-update, merge, delete", foreign_keys="ModelMetadata.model_name")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class User(Base):
    __tablename__ = "user"
    __table_args__ = {"schema": schema}
    user_id = db.Column(db.Integer(), primary_key=True)
    user_name = db.Column(db.String())
    user_email = db.Column(db.String())
    last_login = db.Column(db.DateTime())
    joined =  db.Column(db.DateTime())
    ip = db.Column(db.String())
    notes = db.Column(db.String())
    active = db.Column(db.String())
    role = db.Column(db.String())


    data = relationship("Data", back_populates="user", cascade="save-update, merge, delete")
    feature_collections = relationship("FeatureCollection", back_populates="users", cascade="save-update, merge, delete")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class FeatureCollection(Base):
    __tablename__ = "feature_collection"
    __table_args__ = {"schema": schema}
    feature_collection_id = db.Column(db.Integer(), primary_key=True)
    feature_collection_name =  db.Column(db.String(), unique=True, index=True, nullable=False)
    user_id =  db.Column(db.Integer(), db.ForeignKey(schema + "." + "user.user_id"), nullable=False)
    feature_collection_permission = db.Column(db.String())
    url_path_to_shapefile = db.Column(db.String())

    users = relationship("User", back_populates="feature_collections",cascade="save-update, merge, delete", foreign_keys="FeatureCollection.user_id")
    features = relationship("Feature", back_populates="feature_collections",cascade="save-update, merge, delete")


class Feature(Base):
    __tablename__ = "feature"
    __table_args__ = {"schema": schema}
    feature_id = db.Column(db.Integer(), primary_key=True)
    feature_collection_name = db.Column(db.String(), db.ForeignKey(schema + "." + "feature_collection.feature_collection_name"), index=True, nullable=False)
    feature_id_from_user = db.Column(db.String())
    type = db.Column(db.String())
    year = db.Column(db.Integer())
    geometry = db.Column(Geometry(geometry_type="MULTIPOLYGON"))

    feature_collections = relationship("FeatureCollection", back_populates="features", cascade="save-update, merge, delete", foreign_keys="Feature.feature_collection_name")
    data = relationship("Data", back_populates="feature", cascade="save-update, merge, delete")
    feature_metadata = relationship("FeatureMetadata", back_populates="feature", cascade="save-update, merge, delete")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class FeatureMetadata(Base):
    __tablename__ = "feature_metadata"
    __table_args__ = {"schema": schema}
    feature_metadata_id = db.Column(db.Integer(), primary_key=True)
    feature_id = db.Column(db.Integer(), db.ForeignKey(schema + "." + "feature.feature_id"), nullable=False)
    feature_metadata_name = db.Column(db.String())
    feature_metadata_properties = db.Column(db.String())

    feature = relationship("Feature", back_populates="feature_metadata", cascade="save-update, merge, delete", foreign_keys="FeatureMetadata.feature_id")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Variable(Base):
    __tablename__ = "variable"
    __table_args__ = {"schema": schema}
    variable_id = db.Column(db.Integer(), primary_key=True)
    variable_name = db.Column(db.String(), unique=True, index=True)
    units = db.Column(db.String())

    data = relationship("Data", back_populates="variable", cascade="save-update, merge, delete")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Data(Base):
    __tablename__ = "data"
    __table_args__ = {"schema": schema}
    data_id = db.Column(db.Integer(), primary_key=True)
    feature_id = db.Column(db.Integer(), db.ForeignKey(schema + "." + "feature.feature_id"), nullable=False)
    user_id =  db.Column(db.Integer(), db.ForeignKey(schema + "." + "user.user_id"), nullable=False)
    timeseries_id = db.Column(db.Integer(), db.ForeignKey(schema + "." + "timeseries.timeseries_id"), nullable=False)
    # report_id  = db.Column(db.Integer(), db.ForeignKey(schema + "." + "report.report_id"))
    model_name =  db.Column(db.String(), db.ForeignKey(schema + "." + "model.model_name"), index=True, nullable=False)
    variable_name =  db.Column(db.String(), db.ForeignKey(schema + "." + "variable.variable_name"), index=True, nullable=False)
    temporal_resolution = db.Column(db.String())
    permission = db.Column(db.String())
    last_timeseries_update = db.Column(db.DateTime())

    feature = relationship("Feature", back_populates="data", cascade="save-update, merge, delete", foreign_keys="Data.feature_id")
    user = relationship("User", back_populates="data", cascade="save-update, merge, delete", foreign_keys="Data.user_id")
    timeseries = relationship("Timeseries", back_populates="data", cascade="save-update, merge, delete", foreign_keys="Data.timeseries_id")
    # report = relationship("Report", back_populates="data", cascade="save-update, merge, delete", foreign_keys="Data.report_id")
    model = relationship("Model", back_populates="data", cascade="save-update, merge, delete", foreign_keys="Data.model_name")
    variable = relationship("Variable", back_populates="data", cascade="save-update, merge, delete", foreign_keys="Data.variable_name")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Timeseries(Base):
    __tablename__ = "timeseries"
    __table_args__ = {"schema": schema}
    timeseries_id = db.Column(db.Integer(), primary_key=True)
    start_date = db.Column(db.DateTime())
    end_date = db.Column(db.DateTime())
    data_value = db.Column(db.Float(precision=4))

    data = relationship("Data", back_populates="timeseries", cascade="save-update, merge, delete")

"""
class Parameters(Base):
    __tablename__ = "parameter"
    __table_args__ = {"schema": schema}
    parameter_id = db.Column(db.Integer(), primary_key=True)
    variable_name = db.Column(db.String(), db.ForeignKey(schema + "."  + "variable.variable_name"), index=True, nullable=False)
    model_name = db.Column(db.String(), db.ForeignKey(schema + "."  + "model.model_name"), index=True, nullable=False)
    parameter_name =  db.Column(db.String())
    parameter_properties = db.Column(db.String())

    model = relationship("Model", back_populates="parameters", cascade="save-update, merge, delete", foreign_keys="Parameters.model_name")
    variable = relationship("Variable", back_populates="parameters", cascade="save-update, merge, delete", foreign_keys="Parameters.variable_name")

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Report(Base):
    __tablename__ = "report"
    __table_args__ = {"schema": schema}
    report_id = db.Column(db.Integer(), primary_key=True)
"""

#######################################
# END OpenET database Utility class
#######################################

class database_Util(object):
    """
    Class to support database (postgres+postgis) population
    Method:
        - The base query is defined from relevant template values
    Args:
        :feature_collection Unique ID of geojson file containing fields for the feature_collection
        :model SSEBop etc
        :year year of geojson model, might be ALL if not USFields
            USField geojsons change every year
        :user_id
        :feature_collection_changing_by_year: True or False, if False year = 9999 in Feature table
    """
    def __init__(self, feature_collection, model, year, user_id, feature_collection_changing_by_year, engine):
        self.feature_collection = feature_collection
        self.year = int(year)
        self.model = model
        self.user_id = user_id
        self.geo_bucket_url = GEO_BUCKET_URL
        self.data_bucket_url = DATA_BUCKET_URL
        '''
        if project == "NASA_ROSES":
            self.data_bucket_url = config.NASA_ROSES_DATA_BUCKET_URL
        elif project == "OPENET":
            self.data_bucket_url = config.OPENET_ROSES_DATA_BUCKET_URL
        else:
            raise Exception('Project must be in list: ' + str(config.projects))
        '''
        self.feature_collection_changing_by_year = feature_collection_changing_by_year
        self.engine = engine

        # Used to read geometry data from buckets
        if self.feature_collection_changing_by_year:
            # Field boundaries depend on years
            self.geoFName = feature_collection + "_" + str(year) + ".geojson"
        else:
            self.geoFName = feature_collection + ".geojson"
        self.dataFName = feature_collection + "_" + str(year) + ".json"

    def object_as_dict(self, obj):
        """
        Converts single db query object to dict
        :param obj:
        :return: query dict
        """
        return {c.key: getattr(obj, c.key)
                for c in inspect(obj).mapper.column_attrs}

    def set_shell_flag(self):
        if os.name == "posix":
            shell_flag = False
        else:
            shell_flag = True
        return shell_flag

    def upload_file_to_bucket(self, upload_path, bucket_path):
        """
        :param upload_path: source file path on local host
        :param bucket_path: destination file path
        :return:
        """
        logging.info("Uploading to bucket")
        shell_flag = self.set_shell_flag()
        args = ["gsutil", "cp", upload_path, bucket_path]
        if not logging.getLogger().isEnabledFor(logging.DEBUG):
            args.insert(1, "-q")

        # Check that the file is not already in bucket
        args = ["gsutil", "ls", bucket_path]
        try:
            out = subprocess.check_output(args, shell=shell_flag)
        except Exception as e:
            out = ""
            logging.exception("Error checking file in bucket: " + str(e))

        if not out:
            # Add file to bucket
            try:
                subprocess.check_output(args, shell=shell_flag)
            except Exception as e:
                logging.exception("Error uploading to bucket: " + str(e))

    def delete_file_from_local(self, upload_path):
        try:
            os.remove(upload_path)
            logging.info("Deleted local file " + upload_path)
        except:
            pass

    def delete_file_from_bucket(self, bucket_path):
        logging.info("Deleting bucket")
        shell_flag = self.set_shell_flag()
        args = ["gsutil", "rm", bucket_path]
        if not logging.getLogger().isEnabledFor(logging.DEBUG):
            args.insert(1, "-q")
        try:
            subprocess.check_output(args, shell=shell_flag)
        except Exception as e:
            logging.exception("Error uploading to bucket: " + str(e))


    def read_geodata_from_bucket(self):
        """
        All geometry data are stored in cloud buckets
        :return:
        """
        url = self.geo_bucket_url + self.geoFName
        try:
            d = geojson.load(urllib2.urlopen(url))
        except Exception as e:
            logging.error(e)
            raise Exception(e)
        return d

    def read_etdata_from_bucket(self):
        """
        All et data are stored in cloud buckets
        :return:
        """
        url = self.data_bucket_url + self.model + "/" + self.dataFName
        d = json.load(urllib2.urlopen(url))
        print("Reading data from bucket file " + url)
        """
        try:
            d = json.load(urllib2.urlopen(url))
            print("Read data from bucket file " + url)
        except Exception as e:
            logging.error(e)
            raise Exception(e)
        """
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
            logging.info("ADDING CHUNK {0} of {1}".format(str(count), str(num_chunks)))
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
        is_empty = table_names == [] or len(table_names) == 1
        print("DB is empty: {}".format(is_empty))
        return is_empty

    def table_exists(self, table_name):
        ret = self.engine.dialect.has_table(self.engine, table_name)
        print("Table {} exists: {}".format(table_name, ret))
        return ret

    def check_if_data_in_db(self, feature_id):
        # Check if this entry is already in db
        in_db =  False
        if feature_id is None:
            return in_db

        QU = query_Util({
            "model": self.model,
            "variable": "et",
            "user_id": 0,
            "temporal_resolution": self.temporal_resolution,
            "engine": self.engine

        })
        in_db = QU.check_if_data_in_db(feature_id)
        return in_db


    def check_if_feature_in_db(self, feature_collection, feature_id_from_user, year, session):
        coll_name = config.statics["feature_collections"][feature_collection]["feature_collection_name"]
        feature_query = session.query(Feature).filter(
            Feature.feature_id_from_user == str(feature_id_from_user),
            Feature.year == year,
            Feature.feature_collection_name == coll_name
        )
        if len(feature_query.all()) == 0:
            return None
        if len(feature_query.all()) > 1:
            logging.error("Multiple geometries for " + feature_collection + "/" + str(feature_id_from_user) + "/" + str(year))
            return None
        feature = feature_query.first()
        feature_id = feature.feature_id
        return feature_id

    def set_postgis_geometry(self, shapely_geom):
        postgis_geom = None
        if shapely_geom.geom_type == "Polygon":
            # convert to multi polygon
            postgis_geom = from_shape(MultiPolygon([shapely_geom]))
        elif shapely_geom.geom_type == "MultiPolygon":
            # Save as is
            postgis_geom = from_shape(shapely_geom)
        return postgis_geom

    def set_feature_entity(self, feature_id_from_user, geom_type, postgis_geometry, year):
        """
        Adds the geometry row to database and retrieves the automatically
        assigned primary key geom_id
        # Note: primary key is AUTOSET in db
                feature_id_from user is set to feature_index in featCollection if user didn"t give it
        """
        coll_name = config.statics["feature_collections"][self.feature_collection]["feature_collection_name"]
        feature = Feature(
            feature_collection_name = coll_name,
            feature_id_from_user = feature_id_from_user,
            type = geom_type,
            year = int(year),
            geometry = postgis_geometry
        )
        return feature

    def add_entity_to_db(self, session, entity):
        """
        Add single entity to db
        :param session:
        :param entities:
        :return:
        """
        session.add(entity)
        try:
            session.commit()
        except:
            session.rollback()
            raise

    def add_entities_to_db(self, session, entities):
        """
        Add multiple entities to db
        :param session:
        :param entities:
        :return:
        """
        session.add_all(entities)
        try:
            session.commit()
        except:
            session.rollback()
            raise


    def set_user_dict(self, user_id):
        """
        set the dictonary used to populate db User table  for a single user
        so that adding users is simple
        Note: user parameters need to be stored in config.statics["user"]
        :param user_id:
        :return:
        """
        init_dict = copy.deepcopy(config.statics["users"][user_id])
        # Update date parameters
        init_dict["last_login"] = dt.datetime.today()
        init_dict["joined"] = dt.datetime.today()
        return init_dict

    def add_user_to_db(self, init_dict, session):
        entity = User(**init_dict)
        self.add_entity_to_db(session, entity)

    def set_base_database_tables(self, session):
        # User
        entities = []
        for user_id in config.statics["users"].keys():
            init_dict = copy.deepcopy(self.set_user_dict(user_id))
            entities.append(User(**init_dict))
        self.add_entities_to_db(session, entities)
        print("Added User Table")

        # Model ModelMetadata
        entities = []
        m_entities = []
        for model_name in config.statics["models"].keys():
            init_dict = copy.deepcopy(config.statics["models"][model_name])
            # This goes into the model_metdata table
            del init_dict["variables"]
            del init_dict["metadata"]
            entities.append(Model(**init_dict))

            init_dict = copy.deepcopy(config.statics["models"][model_name]["metadata"])
            init_dict["model_name"] = model_name
            m_entities.append(ModelMetadata(**init_dict))


        self.add_entities_to_db(session, entities)
        self.add_entities_to_db(session, m_entities)
        del m_entities
        print("Added Model and ModelMetadata Tables")

        # Variable
        entities = []
        for var_name in config.statics["variables"].keys():
            init_dict = copy.deepcopy(config.statics["variables"][var_name])
            entities.append(Variable(**init_dict))
        self.add_entities_to_db(session, entities)
        print("Added Variable Table")

        # FeatureCollection
        entities = []
        for coll_name in config.statics["feature_collections"].keys():
            coll_dict = copy.deepcopy(config.statics["feature_collections"][coll_name])
            del coll_dict["metadata"]
            users = coll_dict["users"]
            del coll_dict["users"]
            for user in users:
                init_dict = copy.deepcopy(coll_dict)
                init_dict["user_id"] = user
                entities.append(FeatureCollection(**init_dict))
        self.add_entities_to_db(session, entities)
        print("Added FeatureCollection table")

        """
        # Parameters (depends on model AND variable)
        entities = []
        for model_name in config.statics["parameters"].keys():
            for var_name in config.statics["parameters"][model_name].keys():
                init_dict = copy.deepcopy(config.statics["parameters"][model_name][var_name])
                entities.append(Parameters(**init_dict))
        self.add_entities_to_db(session, entities)
        print("Added Parameter rows")
        """
        # NOTE: Feature, FeatureMetdata tables are set later

    def add_data_to_db(self, session, user_id=0, etdata=None, geojson_data=None):
        """
        Add data to database
        :params:
            session: database session
            user_id
            etdata: json object containing the data, if None, data is read from bucket
            geojson_data: contains the geometry information as geojson, if None, data is read from bucket
        :return:
        """
        # Read etdata from bucket
        if etdata is None:
            etdata = self.read_etdata_from_bucket()

        if geojson_data is None:
            geojson_data = self.read_geodata_from_bucket()

        # Set the user ids associated with this feature_collection
        user_ids_for_featColl = config.statics["feature_collections"][self.feature_collection]["users"]

        # Check if database is empty
        # If not empty, we need to check if entries are already in db
        db_empty = False
        try:
            q = session.query(Data).first()
            if q is None:
                db_empty = True
        except:
            db_empty = True
            # db_empty = self.database_is_empty()



        if db_empty:
            # Set up feature_collection, model, parameter and variable tables
            print("Database empty, setting up basic data tables")
            self.set_base_database_tables(session)


        # Loop over features in bucket file, do in chunks
        # Oherwise we get a kill9 error
        chunk_size = config.statics["ingest_chunk_size"]
        if chunk_size <= len(etdata["features"]):
            num_chunks = len(etdata["features"]) / chunk_size
        else:
            num_chunks = 1

        if len(etdata["features"]) / chunk_size:
            num_chunks += 1
        chunk = 1
        print("Adding data in " + str(num_chunks) + " chunk(s) to database.")
        # Open db connection
        # Needed to bulk copy from csv
        conn = session.connection()  # SQLAlchemy Connection
        dbapi_conn = conn.connection  # DBAPI connection (technically a connection pool wrapper called ConnectionFairy, but everything is there)
        cursor = dbapi_conn.cursor()  # actual DBAPI cursor
        cursor.execute("SET search_path TO myschema," + schema + ", public")

        # FIXME: these should not be hardcoded here
        permission = "public"
        last_timeseries_update = dt.datetime.today()
        report_id = 0
        timeseries_id = 0
        while chunk <= num_chunks:
            csv_metadata = open("metadata.csv", "wb+")
            csv_data = open("data.csv", "wb+")
            csv_timeseries = open("timeseries.csv", "wb+")
            csv_meta_writer = csv.writer(csv_metadata, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
            csv_data_writer = csv.writer(csv_data, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
            csv_ts_writer = csv.writer(csv_timeseries, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)

            idx_start = (chunk - 1) * chunk_size
            idx_end = chunk * chunk_size
            if idx_end > len(etdata["features"]):
                idx_end = len(etdata["features"])

            for f_idx in range(idx_start, idx_end):
                feat_idx = f_idx +1
                print("Adding Feature "  + str(f_idx + 1))
                # Feature table
                # check if the feature is already in the database
                if self.feature_collection_changing_by_year:
                    year = self.year
                else:
                    year = 9999
                g_data = geojson_data["features"][f_idx]
                # Set the feature_id_from_user
                if "feature_id_from_user" in g_data["properties"].keys():
                    feature_id_from_user = str(g_data["properties"]["feature_id_from_user"])
                else:
                    feature_id_from_user = str(feat_idx)

                # Check if feature is in db
                feature_id = self.check_if_feature_in_db(self.feature_collection, str(feat_idx), year, session)
                # Check if  data is in db
                data_in_db = self.check_if_data_in_db(feature_id)

                if feature_id and data_in_db:
                    print("Data for feature_id/year " + str(feature_id) + "/" + str(self.year) + " found in db. Skipping...")
                    continue

                if not feature_id:
                    # Convert the geojson geometry to postgis geometry using shapely
                    # Note: we convert polygons to multi polygon
                    # Convert to shapely shape
                    shapely_geom = asShape(g_data["geometry"])
                    postgis_geom = self.set_postgis_geometry(shapely_geom)
                    if postgis_geom is None:
                        raise Exception("Not a valid geometry, must be polygon or multi polygon!")
                    # Add the feature table entry for this feature and obtain the feature_id
                    feature = self.set_feature_entity(feature_id_from_user, shapely_geom.geom_type, postgis_geom, year)
                    # Submit the feature table to obtain the primary key feature_id
                    self.add_entity_to_db(session, feature)
                    # Get the feature primary key from db
                    feature_id = feature.feature_id
                    logging.info("Added Feature Table")
                    # Add the many-to-many relationship between user and feature
                    # (user_id, feature_id pairs)
                    uid_feat_pairs = []
                    for user_id in user_ids_for_featColl:
                        uid_feat_pairs.append((user_id, feature_id))
                else:
                    logging.info("Feature found in db")
                    print("Feature found in db")

                f_data = etdata["features"][f_idx]
                # Set the feature metadata and data tables for bulk ingest
                for key in config.statics["feature_collections"][self.feature_collection]["metadata"]:
                    try:
                        value = str(g_data["properties"][key])
                    except:
                        try:
                           value = str(f_data["properties"][key])
                        except:
                            value = "Not Found"
                    # Remove commas, causes issues when copy_from
                    value = " ".join(value.replace(", ", ",").split(","))
                    csv_meta_writer.writerow([feature_id, key, value])

                # Variable loop
                for var in config.statics["models"][self.model]["variables"]:
                    for t_res in config.statics["temporal_resolution"].keys():
                        for data_var in config.statics["temporal_resolution"][t_res]["data_vars"]:
                            timeseries_id+=1
                            # Set date
                            DU = date_Util()
                            start_date_dt, end_date_dt = DU.get_dbtable_start_end_dates(self.year, t_res, data_var)
                            # Set data value
                            try:
                                data_value = float(f_data["properties"][var + "_" + data_var])
                            except:
                                data_value = -9999

                            row = [timeseries_id, start_date_dt, end_date_dt, data_value]
                            csv_ts_writer.writerow(row)
                            # row = [feature_id, user_id, timeseries_id, report_id, self.model, var, t_res, permission, last_timeseries_update]
                            row = [feature_id, user_id, timeseries_id, self.model, var, t_res, permission, last_timeseries_update]
                            csv_data_writer.writerow(row)


            session.execute(FeatureUserLink.insert().values(uid_feat_pairs))
            print("Added FeatureUserLink Table")

            csv_metadata.close()
            csv_timeseries.close()
            csv_data.close()


            # Commit the feature metadata and data for all features
            with open("timeseries.csv", "r") as f:
                if os.stat("timeseries.csv").st_size != 0:
                    cols = ("timeseries_id", "start_date", "end_date", "data_value")
                    cursor.copy_from(f, "timeseries", sep=",", columns=cols)
                    print("Added timeseries table rows for features")

            with open("data.csv", "r") as f:
                if os.stat("data.csv").st_size != 0:
                    cols = ("feature_id", "user_id", "timeseries_id",
                            "model_name", "variable_name", "temporal_resolution",
                            "permission", "last_timeseries_update")
                    cursor.copy_from(f, "data", sep=",", columns=cols)
                    print("Added Data tables for features")

            with open("metadata.csv", "r") as f:
                if os.stat("metadata.csv").st_size != 0:
                    cols = ("feature_id", "feature_metadata_name", "feature_metadata_properties")
                    cursor.copy_from(f, "feature_metadata", sep=",", columns=cols)
                    print("Added FeatureMetadata table rows for features")



            try:
                session.commit()
            except:
                session.rollback()
                raise

            # Delete the csv files
            os.remove("metadata.csv")
            os.remove("data.csv")
            os.remove("timeseries.csv")
            chunk += 1
        # Close the connection
        conn.close()


class date_Util(object):

    def get_month(self, t_res, data_var):
        """
        :param t_res: temporal resolution
        :param data_var:  data variable found in data files: for monthly m01, m02, ect.
        :return:
        """
        if t_res == "annual":
            m = 12
        elif t_res == "seasonal":
            m = 10
        elif t_res == "monthly":
            try:
                m = int(data_var.split("m")[1])
            except:
                m = int(data_var)
        else:
            m = 12
        return m

    def set_datetime_dates_list(self, tv_vars):
        dates_list = []
        data_vars = []
        t_res = tv_vars["temporal_resolution"]
        yr = int(tv_vars["year"])
        if t_res == "annual":
            data_vars = ["annual"]
        if t_res == "seasonal":
            data_vars = ["seasonal"]
        if t_res == "monthly":
            months = tv_vars["months"]
            if len(months) == 1 and months[0] == "all":
                months = copy.deepcopy(config.statics["all_months"])
                del months["all"]
                months = sorted(months.keys())
            data_vars = ["m" + str(m) for m in months]

        for data_var in data_vars:
            m = self.get_month(t_res, data_var)
            d = int(config.statics["mon_len"][m - 1])
            dates_list.append([dt.datetime(yr, m, 1), dt.datetime(yr, m, d)])

        return dates_list

    def get_dbtable_datetime(self, year, t_res, data_var):
        """
        :param t_res: temporal resolution
        :param data_var: data variable found in data files: for monthly m01, m02, ect.
        :return:
        """
        yr = int(year)
        m = self.get_month(t_res, data_var)
        d = int(config.statics["mon_len"][m - 1])
        return dt.datetime(yr, m, d)

    def get_dbtable_start_end_dates(self, year, t_res, data_var):
        """
        :param t_res: temporal resolution
        :param data_var: data variable found in data files: for monthly m01, m02, ect.
        :return:
        """
        yr = int(year)
        m = self.get_month(t_res, data_var)
        d = int(config.statics["mon_len"][m - 1])
        start_date = dt.datetime(yr, m, 1)
        end_date = dt.datetime(yr, m, d)
        return start_date, end_date

class query_Util(object):
    """
    Class to support API queries
    """

    def __init__(self, model, variable, user_id, temporal_resolution, engine):
        self.model = model
        self.variable = variable
        self.user_id = user_id
        self.temporal_resolution = temporal_resolution
        self.engine = engine
        self.conn = engine.connect()
        Session = session_module.sessionmaker()
        # Session = scoped_session(sessionmaker())
        Session.configure(bind=self.engine)
        self.session = Session()
        self.session.execute("SET search_path TO " + schema + ', public')
        self.json_data =  {
            "properties": {
                "user_id": user_id,
                "model": self.model,
                "variable":self.variable,
                "temporal_resolution":self.temporal_resolution

            }
        }

    def set_temporal_summary_column(self, temporal_summary):
        if temporal_summary == 'raw':
            return 'Timeseries.data_value'
        elif temporal_summary == 'mean':
            return 'AVG(Timeseries.data_value)'
        elif temporal_summary == 'max':
            return 'MAX(Timeseries.data_value)'
        elif temporal_summary == 'min':
            return 'MIN(Timeseries.data_value)'
        elif temporal_summary == 'sum':
            return 'SUM(Timeseries.data_value)'
        elif temporal_summary == 'median':
            # FIXME: need to write this function
            # https://docs.sqlalchemy.org/en/latest/core/functions.html
            return 'Timeseries.data_value'


    def set_spatial_summary_column(self, temp_data_col, spatial_summary):
        if spatial_summary == 'raw':
            return temp_data_col
        elif spatial_summary == 'mean':
            return 'AVG(' +  temp_data_col +')'
        elif spatial_summary == 'max':
            return 'MAX(' +  temp_data_col + ')'
        elif spatial_summary == 'min':
            return 'MIN(' +  temp_data_col + ')'
        elif spatial_summary == 'sum':
            return 'SUM(' +  temp_data_col + ')'
        elif spatial_summary == 'median':
            # FIXME: need to write this function
            # https://docs.sqlalchemy.org/en/latest/core/functions.html
            return temp_data_col

    def check_if_data_in_db(self, feature_id):
        sql = sqa.text("""
            SELECT
            roses.timeseries.data_value as data_value,
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id

            WHERE
            roses.data.feature_id = %i 
        """ % (feature_id))
        query_data = self.conn.execute(sql)
        if query_data:
            return True
        else:
            return False

    def test(self):
        sql = sqa.text("""
            SELECT
            count(roses.timeseries.data_value) AS the_count,
            AVG(roses.timeseries.data_value) AS avg_1
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id
            LEFT JOIN roses.feature ON roses.feature.feature_id = roses.data.feature_id

            WHERE
            roses.feature.feature_collection_name = '/projects/nasa-roses/BRC_Combined_subset_2009'
            AND roses.timeseries.start_date >= '2003-01-01T00:00:00'::timestamp
            AND roses.timeseries.end_date <= '2003-12-31T00:00:00'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = 'ssebop'
            AND roses.data.variable_name = 'et'
            AND roses.data.temporal_resolution = 'monthly'
            GROUP BY roses.feature.feature_id
        """)
        query_data = self.conn.execute(sql)
        for qd in query_data:
            print(qd)


    def api_ex1_raw(self, **params):
        """
        Request time series for a single field that is not associated with a user
        using the feature_id (unique primary key) directly
        API eaxample 1: average runtime: 0.75 seconds
        FIXME: should we compute the temporal summary iin postgres?
        :param feature_id: Feature.feature_id primary key of feature in database
        :param start_date: date string or dateime object
        :param end_date: date string or dateime object
        :param temporal_summary: max, min, median, mean or sum
        :param output:
        :return:
        """
        # Sanity ccheck on params
        if 'feature_id' not in params.keys():
            return 'ERROR: feature_id must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'

        fid = params['feature_id']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            roses.data.feature_id AS feat_id,
            roses.timeseries.start_date AS sd,
            roses.timeseries.end_date AS ed,
            roses.timeseries.data_value AS dv
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id
        
            WHERE
            roses.data.feature_id = '%s'
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
        """ %(fid, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        data = [list(qd) for qd in query_data]
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Feature ID', 'Start Date', 'End Date', params['temporal_summary']]
        j_data['data'] = data
        return j_data

    def api_ex1(self, **params):
        """
        Request time series for a single field that is not associated with a user
        using the feature_id (unique primary key) directly
        API eaxample 1: average runtime: 0.75 seconds
        FIXME: should we compute the temporal summary iin postgres?
        :param feature_id: Feature.feature_id primary key of feature in database
        :param start_date: date string or dateime object
        :param end_date: date string or dateime object
        :param temporal_summary: max, min, median, mean or sum
        :param output:
        :return:
        """
        # Sanity ccheck on params
        if 'feature_id' not in params.keys():
            return 'ERROR: feature_id must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'

        data_col = self.set_temporal_summary_column(params['temporal_summary'])
        fid = params['feature_id']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            roses.data.feature_id as feat_id,
            %s
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id

            WHERE
            roses.data.feature_id = '%s'
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
            GROUP BY roses.data.feature_id
        """ % (data_col, fid, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        data = [list(qd) for qd in query_data]
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Feature ID', params['temporal_summary']]
        j_data['properties']['format'] = ['Feature ID', params['temporal_summary']]
        j_data['data'] = data
        return j_data

    def api_ex2_raw(self, **params):
        '''
        Request mean monthly values for each feature  in a featureCollection
        FIXME: Compute temp_summary over each feature in db!!
        :param feature_collection_name:
        :param start_date:
        :param end_date:
        :param temporal_summary:
        :return:
        '''
        # Sanity ccheck on params
        if 'feature_collection_name' not in params.keys():
            return 'ERROR: feature_collection_name must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'

        fc = params['feature_collection_name']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            roses.feature.feature_id AS feat_id, 
            roses.timeseries.start_date AS sd,
            roses.timeseries.end_date AS ed,
            roses.timeseries.data_value AS dv
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id
            LEFT JOIN roses.feature ON roses.feature.feature_id = roses.data.feature_id

            WHERE
            roses.feature.feature_collection_name = '%s'
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
            ORDER BY roses.feature.feature_id
        """ %(fc, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        data = [list(qd) for qd in query_data]
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Feature ID', params['temporal_summary']]
        j_data['data'] = data
        return j_data

    def api_ex2(self, **params):
        '''
        Request mean monthly values for each feature  in a featureCollection
        FIXME: Compute temp_summary over each feature in db!!
        :param feature_collection_name:
        :param start_date:
        :param end_date:
        :param temporal_summary:
        :return:
        '''
        # Sanity ccheck on params
        if 'feature_collection_name' not in params.keys():
            return 'ERROR: feature_collection_name must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'

        data_col = self.set_temporal_summary_column(params['temporal_summary'])
        fc = params['feature_collection_name']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            roses.feature.feature_id as feat_id, 
            %s
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id
            LEFT JOIN roses.feature ON roses.feature.feature_id = roses.data.feature_id

            WHERE
            roses.feature.feature_collection_name = '%s'
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
            GROUP BY roses.feature.feature_id
        """ %(data_col, fc, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        data = [list(qd) for qd in query_data]
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Feature ID', params['temporal_summary']]
        j_data['data'] = data
        return j_data

    def api_ex3_raw(self, **params):
        '''
        Request monthly time series for a single field from a featureCollection
        that is selected by feature_property (feature_id)/feature_value
        :return:
        '''
        # Sanity ccheck on params
        if 'feature_collection_name' not in params.keys():
            return 'ERROR: feature_collection_name must be specified'
        if 'feature_metadata_name' not in params.keys():
            return 'ERROR: feature_metadata_name must be specified'
        if 'feature_metadata_properties' not in params.keys():
            return 'ERROR: feature_metadata_properties must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'

        fc = params['feature_collection_name']
        fmn = params['feature_metadata_name']
        fmp = params['feature_metadata_properties']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            roses.feature.feature_id as feat_id,
            timeseries.start_date,
            timeseries.end_date,
            timeseries.data_value
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id
            LEFT JOIN roses.feature_metadata ON roses.feature_metadata.feature_id = roses.data.feature_id
            LEFT JOIN roses.feature ON roses.feature.feature_id = roses.data.feature_id

            WHERE
            roses.feature.feature_collection_name = '%s'
            AND roses.feature_metadata.feature_metadata_name = '%s'
            AND roses.feature_metadata.feature_metadata_properties = '%s'
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
        """ % (fc, fmn, fmp, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        data = [list(qd) for qd in query_data]
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Feature ID', params['temporal_summary']]
        j_data['data'] = data
        return j_data

    def api_ex3(self, **params):
        '''
        Request monthly time series for a single field from a featureCollection
        that is selected by feature_property (feature_id)/feature_value
        :return:
        '''
        # Sanity ccheck on params
        if 'feature_collection_name' not in params.keys():
            return 'ERROR: feature_collection_name must be specified'
        if 'feature_metadata_name' not in params.keys():
            return 'ERROR: feature_metadata_name must be specified'
        if 'feature_metadata_properties' not in params.keys():
            return 'ERROR: feature_metadata_properties must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'

        data_col = self.set_temporal_summary_column(params['temporal_summary'])
        fc = params['feature_collection_name']
        fmn = params['feature_metadata_name']
        fmp = params['feature_metadata_properties']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            roses.feature.feature_id as feat_id,
            %s
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id
            LEFT JOIN roses.feature_metadata ON roses.feature_metadata.feature_id = roses.data.feature_id
            LEFT JOIN roses.feature ON roses.feature.feature_id = roses.data.feature_id
            
            WHERE
            roses.feature.feature_collection_name = '%s'
            AND roses.feature_metadata.feature_metadata_name = '%s'
            AND roses.feature_metadata.feature_metadata_properties = '%s'
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
            GROUP BY roses.feature.feature_id
        """ % (data_col, fc, fmn, fmp, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        data = [list(qd) for qd in query_data]
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Feature ID', params['temporal_summary']]
        j_data['data'] = data
        return j_data

    def api_ex4_raw(self, **params):
        '''
        Request monthly time series for a single field from a featureCollection
        that is selected by feature_property (feature_id)/feature_value
        :return:
        '''
        # Sanity ccheck on params
        if 'feature_collection_name' not in params.keys():
            return 'ERROR: feature_collection_name must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'
        if 'spatial_summary' not in params.keys():
            return 'ERROR: spatial_summary must be specified'

        fc = params['feature_collection_name']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            roses.feature.feature_id AS feat_id,
            ST_AREA(roses.feature.geometry) AS geom,
            roses.timeseries.start_date AS sd,
            roses.timeseries.end_date AS ed,
            roses.timeseries.data_value AS dv
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id 
            LEFT JOIN roses.feature ON roses.feature.feature_id = roses.data.feature_id

            WHERE
            roses.feature.feature_collection_name = '%s' 
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
            ORDER BY roses.feature.feature_id
        """ % (fc, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        # Get the area average
        featsdata = {}
        for qd in query_data:
            if qd[0] not in featsdata.keys():
                featsdata[qd[0]] = {
                    'summ':qd[1] * qd[-1],
                    'total_area': qd[1],
                    'start_date': qd[2],
                    'end_date': qd[3]
                }
            else:
                featsdata[qd[0]]['summ'] += qd[1] * qd[-1]
                featsdata[qd[0]]['total_area'] += qd[1]

        data = []
        for feat_id in featsdata.keys():
            f_data = round((1.0 / featsdata[feat_id]['total_area']) * featsdata[feat_id]['summ'], 4)
            data.append([featsdata[feat_id]['start_date'], featsdata[feat_id]['end_date'], f_data])
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Start Date', 'End Date', 'Area Averaged Data Value']
        j_data['data'] = data
        return j_data


    def api_ex4(self, **params):
        '''
        Request monthly time series for a single field from a featureCollection
        that is selected by feature_property (feature_id)/feature_value
        :return:
        '''
        # Sanity ccheck on params
        if 'feature_collection_name' not in params.keys():
            return 'ERROR: feature_collection_name must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'
        if 'spatial_summary' not in params.keys():
            return 'ERROR: spatial_summary must be specified'

        ts = params['temporal_summary']
        ss = params['spatial_summary']
        # data_col = self.set_spatial_summary_column(self.set_temporal_summary_column(ts), ss)
        data_col = self.set_temporal_summary_column(ts)
        fc = params['feature_collection_name']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            ST_AREA(roses.feature.geometry),
            %s
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id 
            LEFT JOIN roses.feature ON roses.feature.feature_id = roses.data.feature_id

            WHERE
            roses.feature.feature_collection_name = '%s' 
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
            GROUP BY roses.feature.geometry
        """ % (data_col, fc, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        # Get the area average
        data = []
        summ = 0
        total_area = 0
        num_feats = 0
        for qd in query_data:
            summ += qd[0] * qd[1]
            num_feats +=1
            total_area += qd[0]
        # data = round((1.0 / (total_area * num_feats)) * summ, 4)
        data = round((1.0 / total_area) * summ, 4)
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Area Averaged Data Value']
        j_data['data'] = data
        return j_data

    def api_ex5_raw(self, **params):
        '''
        Request monthly time series for a single field from a featureCollection
        that is selected by feature_property (feature_id)/feature_value
        :return:
        '''
        # Sanity ccheck on params
        if 'feature_collection_name' not in params.keys():
            return 'ERROR: feature_collection_name must be specified'
        if 'feature_metadata_name' not in params.keys():
            return 'ERROR: feature_metadata_name must be specified'
        if 'feature_metadata_properties' not in params.keys():
            return 'ERROR: feature_metadata_properties must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'
        fc = params['feature_collection_name']
        fmn = params['feature_metadata_name']
        fmp = params['feature_metadata_properties']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            roses.feature.feature_id as feat_id,
            roses.timeseries.start_date AS sd,
            roses.timeseries.end_date AS ed,
            roses.timeseries.data_value AS dv
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id
            LEFT JOIN roses.feature_metadata ON roses.feature_metadata.feature_id = roses.data.feature_id
            LEFT JOIN roses.feature ON roses.feature.feature_id = roses.data.feature_id

            WHERE
            roses.feature.feature_collection_name = '%s'
            AND roses.feature_metadata.feature_metadata_name = '%s'
            AND roses.feature_metadata.feature_metadata_properties IN %s
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
            ORDER BY roses.feature.feature_id
        """ % (fc, fmn, fmp, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        data = [list(qd) for qd in query_data]
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Feature ID', params['temporal_summary']]
        j_data['data'] = data
        return j_data

    def api_ex5(self, **params):
        '''
        Request monthly time series for a single field from a featureCollection
        that is selected by feature_property (feature_id)/feature_value
        :return:
        '''
        # Sanity ccheck on params
        if 'feature_collection_name' not in params.keys():
            return 'ERROR: feature_collection_name must be specified'
        if 'feature_metadata_name' not in params.keys():
            return 'ERROR: feature_metadata_name must be specified'
        if 'feature_metadata_properties' not in params.keys():
            return 'ERROR: feature_metadata_properties must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'

        data_col = self.set_temporal_summary_column(params['temporal_summary'])
        fc = params['feature_collection_name']
        fmn = params['feature_metadata_name']
        fmp = params['feature_metadata_properties']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            roses.feature.feature_id as feat_id,
            %s
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id
            LEFT JOIN roses.feature_metadata ON roses.feature_metadata.feature_id = roses.data.feature_id
            LEFT JOIN roses.feature ON roses.feature.feature_id = roses.data.feature_id

            WHERE
            roses.feature.feature_collection_name = '%s'
            AND roses.feature_metadata.feature_metadata_name = '%s'
            AND roses.feature_metadata.feature_metadata_properties IN %s
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
            GROUP BY roses.feature.feature_id
        """ % (data_col, fc, fmn, fmp, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        data = [list(qd) for qd in query_data]
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Feature ID', params['temporal_summary']]
        j_data['data'] = data
        return j_data

    def api_ex6_raw(self, **params):
        '''
        Request mean monthly values for each feature  in a featureCollection
        FIXME: Compute temp_summary over each feature in db!!
        :param feature_collection_name:
        :param start_date:
        :param end_date:
        :param temporal_summary:
        :param spatial_summary:
        :return:
        '''
        # Sanity ccheck on params
        if 'feature_collection_name' not in params.keys():
            return 'ERROR: feature_collection_name must be specified'
        if 'selection_geometry' not in params.keys():
            return 'ERROR: selection must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'

        fc = params['feature_collection_name']
        sg = params['selection_geometry']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            roses.feature.feature_id as feat_id, 
            roses.timeseries.start_date AS sd,
            roses.timeseries.end_date AS ed,
            roses.timeseries.data_value AS dv
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id
            LEFT JOIN roses.feature ON roses.feature.feature_id = roses.data.feature_id

            WHERE
            roses.feature.feature_collection_name = '%s'
            AND ST_CONTAINS(ST_GeomFromText('%s'), roses.feature.geometry)
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
            ORDER BY roses.feature.feature_id
        """ %(fc, sg, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        data = [list(qd) for qd in query_data]
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Feature ID', params['temporal_summary']]
        j_data['data'] = data
        return j_data


    def api_ex6(self, **params):
        '''
        Request mean monthly values for each feature  in a featureCollection
        FIXME: Compute temp_summary over each feature in db!!
        :param feature_collection_name:
        :param start_date:
        :param end_date:
        :param temporal_summary:
        :param spatial_summary:
        :return:
        '''
        # Sanity ccheck on params
        if 'feature_collection_name' not in params.keys():
            return 'ERROR: feature_collection_name must be specified'
        if 'selection_geometry' not in params.keys():
            return 'ERROR: selection must be specified'
        if 'start_date' not in params.keys():
            return 'ERROR: start_date must be specified'
        if 'end_date' not in params.keys():
            return 'ERROR: end_date must be specified'
        if 'temporal_summary' not in params.keys():
            return 'ERROR: temporal_summary must be specified'

        data_col = self.set_temporal_summary_column(params['temporal_summary'])
        fc = params['feature_collection_name']
        sg = params['selection_geometry']
        sd = params['start_date']
        ed = params['end_date']
        sql = sqa.text("""
            SELECT
            roses.feature.feature_id as feat_id, 
            %s
            FROM
            roses.timeseries
            LEFT JOIN roses.data ON roses.data.timeseries_id = roses.timeseries.timeseries_id
            LEFT JOIN roses.feature ON roses.feature.feature_id = roses.data.feature_id

            WHERE
            roses.feature.feature_collection_name = '%s'
            AND ST_CONTAINS(ST_GeomFromText('%s'), roses.feature.geometry)
            AND roses.timeseries.start_date >= '%s'::timestamp
            AND roses.timeseries.end_date <= '%s'::timestamp
            AND roses.data.user_id = 0
            AND roses.data.model_name = '%s'
            AND roses.data.variable_name = '%s'
            AND roses.data.temporal_resolution = '%s'
            GROUP BY roses.feature.feature_id
            ORDER BY roses.feature.feature_id
        """ %(data_col, fc, sg, sd, ed, self.model, self.variable, self.temporal_resolution))
        query_data = self.conn.execute(sql)
        data = [list(qd) for qd in query_data]
        j_data = copy.deepcopy(self.json_data)
        j_data['properties'].update(params)
        j_data['properties']['format'] = ['Feature ID', params['temporal_summary']]
        j_data['data'] = data
        return j_data


# Utility functions:
def datetimes_from_dates(start_date, end_date):
    if isinstance(start_date, basestring):
        start_date_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = dt.datetime.strptime(end_date, "%Y-%m-%d")
    elif isinstance(start_date, dt.date):
        start_date_dt = start_date
        end_date_dt = end_date
    return start_date_dt, end_date_dt

def datetimes_to_dates(start_date_dt, end_date_dt):
    start_date =  dt.datetime.strftime(start_date_dt, "%Y-%m-%d")
    end_date =  dt.datetime.strftime(end_date_dt, "%Y-%m-%d")
    return start_date, end_date

def compute_statistic(data_vals, statistic, fill_value=-9999):
    np_data = np.ma.masked_array(data_vals, data_vals == fill_value)

    if statistic is None:
        return data_vals
    elif statistic == "sum":
        return round(np.sum(np_data), 4)
    elif statistic == "mean":
        return round(np.mean(np_data), 4)
    elif statistic == "max":
        return round(np.max(np_data), 4)
    elif statistic == "min":
        return round(np.min(np_data), 4)
    elif statistic == "median":
        return round(np.median(np_data), 4)

def format_feature_result(query_data, temporal_summary):
    """
    :param query_data: List of tuples (date_start_dt, date_end_dt, data_value)
    :param temporal_summary:
    :return: json object
    """
    format = ["Start Date", "End Date", temporal_summary.upper()]

    if not query_data:
        return format, json.dumps([], ensure_ascii=False).encode("utf8")

    data = []
    # No temporal summary
    if temporal_summary == 'raw':
        return format, [[qd[0].strftime("%Y-%m-%d"), qd[1].strftime("%Y-%m-%d"), qd[-1]] for qd in query_data]

    zipped = zip(*[list(qd) for qd in query_data])
    data_vals = list(zipped[-1])
    data_val = compute_statistic(data_vals, temporal_summary, fill_value=-9999)
    start_date = zipped[1][0].strftime("%Y-%m-%d")
    end_date = zipped[-2][-1].strftime("%Y-%m-%d")
    data.append([start_date, end_date, data_val])
    return format, data

