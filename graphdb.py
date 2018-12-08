#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.SYNOPSIS
  Class to store data to the GraphDB Neo4J
  
.DESCRIPTION

.NOTES
  Author: Daniël Overdevest
"""

from neo4j.v1 import GraphDatabase

class dutchwatergraph(object):

    def __init__(self, uri, truncate=False, user=None, password=None):
        if user:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
        else:
            self._driver = GraphDatabase.driver(uri)
        if truncate:
            self._driver.session().run("MATCH (n) DETACH DELETE n")
            self.createDataModelandIndexes()

    def createDataModelandIndexes(self):
        indexes = ['Route', 'Fairway', 'Bridge', 'Lock', 'ISRS']
        for index in indexes:
            self._driver.session().run(("CREATE INDEX ON :{index}(Id);".format(index=index)))

    def close(self):
        self._driver.close()

    def createNodes(self, data):
        self.createRoute(data.route)
        self.createFairway(data.fairway)
        self.createISRS(data.isrs)
        self.createBridge(data.bridge)
        self.createLock(data.lock)
        self.chainFairway()
        self.chainBridges()
        self.spatialTree()
        return self
        
    def createRoute(self, nodeList):
        for subset in range(0, (int)(len(nodeList) / 1000) + 1):
            f = subset * 1000
            t = min(len(nodeList), (subset + 1) * 1000)
            self._createRoute(nodeList[f:t])
        return self

    def _createRoute(self, nodeList):
        with self._driver.session() as session:
            session.run("""
                        WITH $nodeList as nodeList
                        UNWIND nodeList as nl
                        CREATE (a:Route)
                        SET 
                            a.Code = nl.Code,
                            a.Description = nl.Description,
                            a.ForeignCode = nl.ForeignCode,
                            a.GeoGeneration = nl.GeoGeneration,
                            a.GeoType = nl.GeoType,
                            a.geometry = nl.Geometry,
                            a.Id = nl.Id,
                            a.Name = nl.Name,
                            a.RouteKmBegin = nl.RouteKmBegin,
                            a.RouteKmEnd = nl.RouteKmEnd,
                            a.VinCode = nl.VinCode,
                            a.WaterName = nl.WaterName
                        RETURN "" as None
                        """
                        , nodeList=nodeList)
        return self

    def createFairway(self, nodeList):
        for subset in range(0, (int)(len(nodeList) / 1000) + 1):
            f = subset * 1000
            t = min(len(nodeList), (subset + 1) * 1000)
            self._createFairway(nodeList[f:t])
        return self

    def _createFairway(self, nodeList):
        with self._driver.session() as session:
            session.run("""
                        WITH $nodeList as nodeList
                        UNWIND nodeList as nl
                        CREATE (a:Fairway)
                        SET a.Direction = nl.Direction,
                            a.FairwayNumber = nl.FairwayNumber,
                            a.ForeignCode = nl.ForeignCode,
                            a.GeoGeneration = nl.GeoGeneration,
                            a.GeoType = nl.GeoType,
                            a.geometry = nl.Geometry,
                            a.Id = nl.Id,
                            a.Name = nl.Name,
                            a.RouteId = nl.RouteId,
                            a.RouteKmBegin = nl.RouteKmBegin,
                            a.RouteKmEnd = nl.RouteKmEnd,
                            a.VinCode = nl.VinCode
                        WITH a
                        SET a.km = a.RouteKmEnd - a.RouteKmBegin
                        WITH a
                        MATCH (r:Route {Id: a.RouteId})
                        MERGE (a)-[:PART_OF]->(r)
                        RETURN "" as None
                        """
                        , nodeList=nodeList)

    def createISRS(self, nodeList):
        for subset in range(0, (int)(len(nodeList) / 1000) + 1):
            f = subset * 1000
            t = min(len(nodeList), (subset + 1) * 1000)
            self._createISRS(nodeList[f:t])
        return self

    def _createISRS(self, nodeList):
        with self._driver.session() as session:
            session.run("""
                        WITH $nodeList as nodeList
                        UNWIND nodeList as nl
                        CREATE (a:ISRS)
                        SET a.Code = nl.Code,
                            a.CountryCode = nl.CountryCode,
                            a.FairwayRouteId = nl.FairwayRouteId,
                            a.Function = nl.Function,
                            a.GeoGeneration = nl.Geogeneration,
                            a.GeoType = nl.GeoType,
                            a.geometry = nl.Geometry,
                            a.Hectometer = nl.Hectometer,
                            a.Id = nl.Id,
                            a.Name = nl.Name,
                            a.ObjectName = nl.ObjectName,
                            a.PositionCode = nl.PositionCode,
                            a.SectionNode = nl.SectionNode,
                            a.TerminalCode = nl.TerminalCode,
                            a.UnLocationCode = nl.UnlocationCode
                        RETURN "" as None
                        """
                        , nodeList=nodeList)

    def createBridge(self, nodeList):
        for subset in range(0, (int)(len(nodeList) / 1000) + 1):
            f = subset * 1000
            t = min(len(nodeList), (subset + 1) * 1000)
            self._createBridge(nodeList[f:t])
        return self

    def _createBridge(self, nodeList):
        with self._driver.session() as session:
            session.run("""
                        WITH $nodeList as nodeList
                        UNWIND nodeList as nl
                        CREATE (a:Bridge)
                        SET a.AdministrationId = nl.AdministrationId,
                            a.CanOpen = nl.CanOpen,
                            a.City = nl.City,
                            a.Condition = nl.Condition,
                            a.FairwayId = nl.FairwayId,
                            a.FairwaySectionId = nl.FairwaySectionId,
                            a.ForeignCode = nl.ForeignCode,
                            a.GeoGeneration = nl.GeoGeneration,
                            a.GeoType = nl.GeoType,
                            a.geometry = nl.Geometry,
                            a.HasOpeningOnOtherFairway = nl.HasOpeningsOnOtherFairway,
                            a.Id = nl.Id,
                            a.IsRemoteControlled = nl.IsRemoteControlled,
                            a.IsrsId = nl.IsrsId,
                            a.Length = nl.Length,
                            a.MhwOffset = nl.MhwOffset,
                            a.MhwReferenceLevel = nl.MhwReferenceLevel,
                            a.Name = nl.Name,
                            a.NumberOfOpenings = nl.NumberOfOpenings,
                            a.OperatingTimesId = nl.OperatingTimesId,
                            a.PhoneNumber = nl.PhoneNumber,
                            a.Referencelevel = nl.ReferenceLevel,
                            a.RelatedBuildingComplexName = nl.ReletedBuildingComplexName,
                            a.Rotation = nl.Rotation,
                            a.RouteId = nl.RouteId,
                            a.RouteKmBegin = nl.RouteKmBegin,
                            a.RouteKmEnd = nl.RouteKmEnd,
                            a.VinCode = nl.VinCode,
                            a.Width = nl.Width

                        WITH a
                        MATCH (r:Route {Id: a.RouteId})
                        MERGE (a)-[:LINKED_TO]->(r)
                        WITH a
                        MATCH (f:Fairway {Id: a.FairwayId})
                        MERGE (a)-[:LINKED_TO]->(f)
                        WITH a
                        MATCH (isr:ISRS {Id: a.IsrsId})
                        MERGE (a)-[:LINKED_TO]->(isr)
                        RETURN "" as None
                        """
                        , nodeList=nodeList)

    def createLock(self, nodeList):
        for subset in range(0, (int)(len(nodeList) / 1000) + 1):
            f = subset * 1000
            t = min(len(nodeList), (subset + 1) * 1000)
            self._createLock(nodeList[f:t])
        return self

    def _createLock(self, nodeList):
        with self._driver.session() as session:
            session.run("""
                        WITH $nodeList as nodeList
                        UNWIND nodeList as nl
                        CREATE (a:Lock)
                        SET a.Address = nl.Address,
                            a.AdministrationId = nl.AdministrationId,
                            a.City = nl.City,
                            a.Condition = nl.Condition,
                            a.FairwayId = nl.FairwayId,
                            a.FairwaySectionId = nl.FairwaySectionId,
                            a.ForeignCode = nl.ForeignCode,
                            a.GeoGeneration = nl.GeoGeneration,
                            a.GeoType = nl.GeoType,
                            a.geometry = nl.Geometry,
                            a.Id = nl.Id,
                            a.IsRemoteControlled = nl.IsRemoteControlled,
                            a.IsrsId = nl.IsrsId,
                            a.Length = nl.Length,
                            a.Name = nl.Name,
                            a.NumberOfChambers = nl.NumberOfChambers,
                            a.OperatingTimesId = nl.OperatingTimesId,
                            a.PhoneNumber = nl.PhoneNumber,
                            a.PostalCode = nl.PostalCode,
                            a.ReferenceLevelBeBu = nl.ReferenceLevelBeBu,
                            a.ReferenceLevelBoBi = nl.ReferenceLevelBoBi,
                            a.RelatedBuildingComplexName = nl.RelatedBuildingComplexName,
                            a.RouteId = nl.RouteId,
                            a.RouteKmBegin = nl.RouteKmBegin,
                            a.RouteKmEnd = nl.RouteKmEnd,
                            a.VinCode = nl.VinCode

                        WITH a
                        MATCH (r:Route {Id: a.RouteId})
                        MERGE (a)-[:LINKED_TO]->(r)
                        WITH a
                        MATCH (f:Fairway {Id: a.FairwayId})
                        MERGE (a)-[:LINKED_TO]->(f)
                        WITH a
                        MATCH (isr:ISRS {Id: a.IsrsId})
                        MERGE (a)-[:LINKED_TO]->(isr)
                        RETURN "" as None
                        """
                        , nodeList=nodeList)

    def chainFairway(self):
        with self._driver.session() as session:
            session.run("""
                        MATCH p=(f1:Fairway)-[:PART_OF]->(r:Route)<-[:PART_OF]-(f2:Fairway)
                        WHERE f2.FairwayNumber = (f1.FairwayNumber + 1)
                        MERGE (f1)-[:STREAMS]->(f2)
                        """)
        return self

    def chainBridges(self):
        with self._driver.session() as session:
            session.run("""
                        MATCH (b)
                        WHERE (b:Bridge or b:Lock) and not (b)-[:NEXT]->(:Bridge) and not (b)-[:NEXT]->(:Lock)
                        MATCH (b)-[:LINKED_TO]->(:Route)<-[:LINKED_TO]-(bo)
                        WHERE b.RouteKmBegin < bo.RouteKmBegin and (bo:Bridge or bo:Lock)
                        WITH b, min(bo.RouteKmBegin) as nextBridge
                        MATCH (b)-[:LINKED_TO]->(:Route)<-[:LINKED_TO]-(bo)
                        WHERE bo.RouteKmBegin = nextBridge and (bo:Bridge or bo:Lock)
                        MERGE (b)-[n:NEXT]->(bo)
                        SET n.km = bo.RouteKmBegin - b.RouteKmBegin
                        """)
        return self

    def spatialTree(self):
        queries = ["CALL spatial.addLayer('dwg', 'wkt', '');" ,
                   """
                    MATCH (n:Fairway)
                    CALL spatial.addNode('dwg',n) YIELD node
                    RETURN "";
                    """,
                    "CALL spatial.addLayer('dwg-route', 'wkt', '');" ,
                   """
                    MATCH (n:Route)
                    CALL spatial.addNode('dwg-route',n) YIELD node
                    RETURN "";
                    """]
        for query in queries:
            with self._driver.session() as session:
                session.run(query)
        return self


        