times <- read.table("comp_times.txt", header = TRUE, sep="\t")
sub <- subset(times, size == 102400)
samples <- nrow(sub)
name <- paste("Compression speed for 100kb blocks (",samples," samples)", sep="")
boxplot(sub[c(3:11)], outline=FALSE, log="y", ylim=c(1, 1000000000), 
        main=name, ylab="nanoseconds", xlab="algorithm", yaxt="n")
axis(2, at = c(1, 1e3, 1e6, 1e09))
for (i in c(1, 1000, 1e6, 1e9)) {
  abline(h=i, lty=3)
}