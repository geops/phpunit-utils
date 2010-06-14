import sys
from optparse import OptionParser
import getpass
import os.path

import psycopg2
import psycopg2.extras
import psycopg2.extensions

import xml.etree.ElementTree as ET

from phpunit import _version_ as phpunit_version

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

def cmdline_parser():

  usage = """
  usage: %prog [options] <deffile> [<outfile>]

  deffile is a file which defines the select statements
  to generate the XMLDataset from.

  the format is
  # this is a comment
  tablename|select statement
  
  the statement will be read until the end of the line.
  ALWAYS SELECT ALL COLUMNS

  example:
  myschema.mytable | select * from myschema.mytable where id < 777

  The resulting XMLDatset will be written to stdout.
  """ 


  parser = OptionParser(usage, version="%prog " + phpunit_version)

  # operation options
  parser.add_option("-n", "--schema", dest="schema",
    help="", metavar="SCHEMA")
  parser.add_option("-t", "--table", dest="table",
    help="", metavar="TABLE")

  # connection options
  parser.add_option("-H", "--host", dest="host",
    help="name of db host", metavar="HOSTNAME")
  parser.add_option("-d", "--dbname", dest="dbname",
    help="name of database to connect to", metavar="DBNAME")
  parser.add_option("-p", "--port", dest="port",
    help="port of the database server", metavar="PORT")
  parser.add_option("-U", "--username", dest="username",
    help="username to use for login on the database server",
    metavar="NAME")
  parser.add_option("-w", "--no-password", action="store_true",
    dest="no_password",help="Do not ask for password")

  return parser.parse_args()


def cmdline_err(msg):
  print("%s. see --help" % msg)
  sys.exit(1)


def connect_db(options):
  dbstr = ""
  if options.username:
    dbstr += ' user=%s' % options.username
  if options.port:
    dbstr += ' port=%s' % options.port
  if options.dbname:
    dbstr += ' dbname=%s' % options.dbname
  if not options.no_password:
    dbstr += " password='%s'" % getpass.getpass()
  if options.host:
    dbstr += " host=%s" % options.host

  return psycopg2.connect(dbstr)


def parse_deffile(filename):
  fh = open(filename, 'r')

  if not fh:
    cmdline_error("could not open the deffile")

  i=0
  for line in fh:
    # quick and dirty parser
    lc=line.strip()

    if not lc.startswith("#") and not lc=="":
      pos = lc.find("|") 
      if pos == -1:
        sys.stderr.write("could not find | in line %d\n" % i)
      else:
        tablename = lc[:pos]
        stmnt     = lc[pos+1:] 

        yield (tablename, stmnt)
    i +=  1


def generate_dataset(deffile, db, outfile=sys.stdout):

  #cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
  cur = db.cursor()

  # prepare our xml tree
  root = ET.Element("dataset")

  for tablename, stmnt in parse_deffile(deffile):
    table = ET.SubElement(root,"table")
    table.set("name", tablename)

    cur.execute(stmnt)
    rows = cur.fetchall()

    d = cur.description
    for dd in d:
      column = ET.SubElement(table, "column")
      column.text = dd[0]
    
    for r in rows:
      row = ET.SubElement(table, "row")
      for col in r:
        if col:
          value = ET.SubElement(row, "value")
          if type(col) == unicode:
            value.text = col
          elif type(col) == str:
            value.text = unicode(col, encoding='UTF8')
          elif type(col) == float:
            value.text = "%.9f" % col
          else:
            value.text = str(col)
        else:
          null = ET.SubElement(row, "null")
      
  et = ET.ElementTree(root)
  et.write(outfile, 'UTF8')


def run():
  options, args = cmdline_parser()

  if len(args) == 0:
    cmdline_err("please specifiy a deffile")

  deffile = args[0]
  outfile = sys.stdout
  if len(args) > 1:
    outfile = args[1]

  if not os.path.isfile(deffile):
    cmdline_err("the deffile %s does not exist or is no file" % deffile)

  try:
    db = connect_db(options)
    db.set_client_encoding('UTF8')
  except psycopg2.OperationalError,e :
    print e.message
    sys.exit(1)

  generate_dataset(deffile, db, outfile)
    

