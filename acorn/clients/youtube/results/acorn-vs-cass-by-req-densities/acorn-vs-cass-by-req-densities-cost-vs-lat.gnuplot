# Tested with gnuplot 4.6 patchlevel 6

FN_OUT = system("echo $FN_OUT")
LABEL_Y = system("echo $LABEL_Y")
Y_COL = system("echo $Y_COL")
# Cast to a number
Y_COL = Y_COL + 0

set terminal pdfcairo enhanced size 3in, 2in
set output FN_OUT

set xlabel "Cost ($)"
set ylabel LABEL_Y offset 0.5,0

set border back
set grid back
set xtics scale 0.5,0
set ytics scale 0.5,0
set tics back

set xrange [0:]

set key top left

set pointsize 0.4

# Instance store cost from Mutants. / GB / Month
INST_STORE_COST=0.527583
XDC_NETWORK_COST=0.02

# column 3 is eth0_tx, 14 is storage used in MB
cost(a)=(XDC_NETWORK_COST * column(3) / 1024 / 1024 / 1024 + INST_STORE_COST * column(14) / 1024 * 6)

plot \
"data-cass"  u (cost(0)):(column(Y_COL))                 w lp pt 6 lt 0 lc rgb "blue" not , \
"data-cass"  u (cost(0)):($16 == 0 ? column(Y_COL): 1/0) w lp pt 7      lc rgb "blue" t "Cassandra", \
"data-acorn" u (cost(0)):(column(Y_COL))                 w lp pt 7 lt 0 lc rgb "red"  not, \
"data-acorn" u (cost(0)):($16 == 0 ? column(Y_COL): 1/0) w lp pt 7      lc rgb "red"  t "Acorn (U+T)"
