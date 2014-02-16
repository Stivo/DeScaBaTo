sizeplot <- function (end) {
  sizes <- read.table("comp_sizes.txt", header = TRUE, sep="\t")
  filtered <- subset(sizes, size >= 102400 & ending == end)
  sub <- filtered[c(3:11)] / rep(filtered[c(2)])
  samples <- nrow(sub)
  #min <- min(filtered[c("size")])
  #max <- max(filtered[c("size")])
  name <- paste("sizes for 100 kilobyte blocks for .",end, " (", samples, " samples)", sep="")
  svg(file=paste(name,".svg", sep=""), onefile=TRUE)
  medians <- apply(sub, 2, median)
  min <- min(medians)
  boxplot(sub, outline=FALSE, 
          main=name, 
          ylab="Compression Ratio", xlab="algorithm",
          ylim=c(0,1.2))
  abline(h=min, lty=3)
  grid(nx=NA, ny=NULL)
  dev.off()
}